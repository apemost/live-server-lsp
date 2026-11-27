use rusty_live_server::{Dir, Error, File, FileSystemInterface, Signal};
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use tokio::fs::{read_dir, File as TokioFile, ReadDir};
use tokio::io::AsyncReadExt;
use tokio::sync::{watch, Mutex, RwLock};
use tokio::task::JoinHandle;
use tower_lsp::lsp_types::{
    CodeAction, CodeActionKind, CodeActionOrCommand, CodeActionParams, CodeActionResponse, Command,
    DidChangeTextDocumentParams, DidCloseTextDocumentParams, DidOpenTextDocumentParams,
    DidSaveTextDocumentParams, ExecuteCommandParams, InitializeParams, InitializeResult,
    InitializedParams, MessageType, Position, SaveOptions, ServerCapabilities, ShowDocumentParams,
    TextDocumentSyncCapability, TextDocumentSyncKind, TextDocumentSyncOptions,
    TextDocumentSyncSaveOptions, Url,
};

use tower_lsp::jsonrpc::Error as LspError;
use tower_lsp::{Client, LanguageServer, LspService, Server};

use crate::Config;

struct Backend {
    port: Arc<RwLock<u16>>,
    public: Arc<RwLock<bool>>,
    remote_preview: RwLock<Option<RemotePreview>>,
    eager: Arc<RwLock<bool>>,
    client: Client,
    threads: Arc<Mutex<Vec<JoinHandle<()>>>>,
    workspace_folders: Arc<Mutex<HashMap<PathBuf, (String, LspFileService)>>>,
}

#[derive(Clone)]
struct RemotePreview {
    local_port: u16,
    listener: watch::Sender<Option<Result<(), String>>>,
}

#[derive(Clone)]
struct LspFileService {
    eager: bool,
    port: Arc<Mutex<u16>>,
    root: Arc<PathBuf>,
    files: Arc<Mutex<HashMap<PathBuf, String>>>,
    sig: Signal,
}

struct LspDir {
    dir: ReadDir,
}

enum LspFile {
    Content(String),
    File(TokioFile),
}

impl LspFile {
    async fn new(
        files: Arc<Mutex<HashMap<PathBuf, String>>>,
        path: &Path,
        eager: bool,
    ) -> Result<Self, Error> {
        if !eager {
            return Ok(LspFile::File(TokioFile::open(path).await?));
        }
        let content = files.lock().await.get(path).cloned();
        Ok(match content {
            Some(v) => LspFile::Content(v.to_string()),
            None => LspFile::File(TokioFile::open(path).await?),
        })
    }
}

impl File for LspFile {
    async fn read_to_end(&mut self) -> Vec<u8> {
        match self {
            LspFile::Content(c) => c.as_bytes().to_vec(),
            LspFile::File(file) => {
                let mut buffer = vec![];
                let _ = file.read_to_end(&mut buffer).await;
                buffer
            }
        }
    }
}

impl LspDir {
    async fn new(path: &Path) -> Result<Self, Error> {
        let dir = read_dir(path).await?;
        Ok(Self { dir })
    }
}

impl Dir for LspDir {
    async fn get_next(&mut self) -> Result<Option<PathBuf>, Error> {
        Ok(self.dir.next_entry().await?.map(|v| v.path()))
    }
}

impl FileSystemInterface for LspFileService {
    async fn get_dir(&self, path: &Path) -> Result<impl Dir, Error> {
        LspDir::new(path).await
    }

    async fn get_file(&self, path: &Path) -> Result<impl File, Error> {
        LspFile::new(self.files.clone(), path, self.eager).await
    }
}

#[tower_lsp::async_trait]
impl LanguageServer for Backend {
    async fn did_open(&self, params: DidOpenTextDocumentParams) {
        let uri = params.text_document.uri;
        let Some(file_path) = self.uri_to_file_path(&uri).await else {
            return;
        };
        let content = params.text_document.text;

        if let Some((_, service)) = self.get_workspace_for_file(&file_path).await {
            let mut files = service.files.lock().await;
            if *self.eager.read().await {
                files.insert(file_path.clone(), content.clone());
            }
            self.update_file(&file_path, &service, false).await;
        }
    }

    async fn did_save(&self, params: DidSaveTextDocumentParams) {
        let uri = params.text_document.uri;
        let Some(file_path) = self.uri_to_file_path(&uri).await else {
            return;
        };
        if let Some((_, service)) = self.get_workspace_for_file(&file_path).await {
            let message = format!("File saved: {}", uri);
            self.client.log_message(MessageType::INFO, message).await;
            self.update_file(&file_path, &service, true).await;
        }
    }

    async fn did_change(&self, params: DidChangeTextDocumentParams) {
        let uri = params.text_document.uri;
        let Some(file_path) = self.uri_to_file_path(&uri).await else {
            return;
        };

        if let Some((_, service)) = self.get_workspace_for_file(&file_path).await {
            let mut files = service.files.lock().await;
            if let Some(file) = files.get_mut(&file_path) {
                if *self.eager.read().await {
                    for change in params.content_changes {
                        if let Some(range) = change.range {
                            let start = get_byte_index_from_position(file, range.start);
                            let end = get_byte_index_from_position(file, range.end);

                            file.replace_range(start..end, &change.text);
                        } else {
                            *file = change.text.clone();
                        }
                    }
                }
            }
            self.update_file(&file_path, &service, false).await;
        }
    }

    async fn execute_command(
        &self,
        params: ExecuteCommandParams,
    ) -> tower_lsp::jsonrpc::Result<Option<Value>> {
        if params.command != "openProjectWeb" {
            return Err(LspError::method_not_found());
        }

        let [project, file, ..] = params.arguments.as_slice() else {
            return Err(LspError::invalid_params("URL argument missing"));
        };
        let (Some(project), Some(file)) = (project.as_str(), file.as_str()) else {
            return Err(LspError::invalid_params("URL argument missing"));
        };
        let project = Path::new(project);
        let file = Path::new(file);
        let remote_port = self
            .workspace_folders
            .lock()
            .await
            .get(project)
            .map(|(_, service)| service.port.clone())
            .ok_or_else(|| LspError::invalid_params("URL argument invalid"))?;

        let remote_preview = self.remote_preview.read().await.clone();
        if let Some(preview) = remote_preview {
            let url = remote_preview_url(preview.local_port, file)?;
            let mut listener = preview.listener.subscribe();
            let status = listener
                .wait_for(Option::is_some)
                .await
                .map_err(|_| preview_error("LiveServer stopped before opening the preview"))?
                .as_ref()
                .expect("listener status is set")
                .clone();
            status.map_err(preview_error)?;

            let request = ShowDocumentParams {
                uri: url.clone(),
                external: Some(true),
                take_focus: None,
                selection: None,
            };
            if !matches!(self.client.show_document(request).await, Ok(true)) {
                let message =
                    format!("Could not open the preview in the client. Open {url} manually.");
                self.client
                    .show_message(MessageType::WARNING, &message)
                    .await;
                return Err(preview_error(message));
            }
        } else {
            let file = make_file_url_path(project, file);
            let port = *remote_port.lock().await;
            if let Err(error) = webbrowser::open(&format!("http://127.0.0.1:{port}/{file}")) {
                self.client
                    .show_message(
                        MessageType::WARNING,
                        format!("failed to open browser {error}"),
                    )
                    .await;
                return Err(LspError::invalid_params("failed to open browser"));
            }
        }
        Ok(None)
    }

    async fn initialize(
        &self,
        params: InitializeParams,
    ) -> tower_lsp::jsonrpc::Result<InitializeResult> {
        let options = params.initialization_options.unwrap_or_default();
        let ssh_preview = options
            .get("local_port")
            .is_some_and(|value| !value.is_null());
        let config: Config = match serde_json::from_value(options) {
            Ok(config) => config,
            Err(error) if ssh_preview => {
                return Err(LspError::invalid_params(format!(
                    "Invalid SSH preview options: {error}"
                )));
            }
            Err(_) => Config::default(),
        };
        if let Some(local_port) = config.local_port {
            if local_port == 0 || config.start_port == Some(0) {
                return Err(LspError::invalid_params(
                    "SSH preview ports must be nonzero",
                ));
            }
            if config.public == Some(true) {
                return Err(LspError::invalid_params(
                    "SSH preview requires loopback binding",
                ));
            }
            if params.workspace_folders.as_ref().map_or(0, Vec::len) != 1 {
                return Err(LspError::invalid_params(
                    "SSH preview requires one workspace folder",
                ));
            }
            *self.remote_preview.write().await = Some(RemotePreview {
                local_port,
                listener: watch::channel(None).0,
            });
        }
        {
            *self.eager.write().await = !config.lazy.unwrap_or_default();
            *self.port.write().await = config.start_port.unwrap_or(57391);
            *self.public.write().await = config.public.unwrap_or_default();
        }

        if let Some(workspace_folders) = params.workspace_folders {
            let mut folders = self.workspace_folders.lock().await;
            for folder in workspace_folders {
                let name = if folder.name.is_empty() {
                    folder
                        .uri
                        .to_file_path()
                        .ok()
                        .and_then(|path| {
                            path.file_name()
                                .map(|name| name.to_string_lossy().into_owned())
                        })
                        .unwrap_or_else(|| "Unnamed Workspace".to_string())
                } else {
                    folder.name.clone()
                };
                let path = folder
                    .uri
                    .to_file_path()
                    .unwrap_or_else(|_| PathBuf::from(&folder.uri.to_string()));
                let fs = LspFileService {
                    port: Arc::new(Mutex::new(*self.port.read().await)),
                    sig: Signal::default(),
                    eager: *self.eager.read().await,
                    files: Default::default(),
                    root: Arc::new(path.clone()),
                };
                folders.insert(path, (name, fs));
            }
        }
        Ok(InitializeResult {
            capabilities: ServerCapabilities {
                code_action_provider: Some(
                    tower_lsp::lsp_types::CodeActionProviderCapability::Simple(true),
                ),
                execute_command_provider: Some(tower_lsp::lsp_types::ExecuteCommandOptions {
                    commands: vec!["openProjectWeb".to_string()],
                    ..Default::default()
                }),

                text_document_sync: Some(TextDocumentSyncCapability::Options(
                    match *self.eager.read().await {
                        true => TextDocumentSyncOptions {
                            open_close: Some(true),
                            change: Some(TextDocumentSyncKind::INCREMENTAL),
                            ..Default::default()
                        },
                        false => TextDocumentSyncOptions {
                            open_close: Some(false),
                            change: Some(TextDocumentSyncKind::NONE),
                            save: Some(TextDocumentSyncSaveOptions::SaveOptions(SaveOptions {
                                include_text: Some(false),
                            })),
                            ..Default::default()
                        },
                    },
                )),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    async fn code_action(
        &self,
        params: CodeActionParams,
    ) -> tower_lsp::jsonrpc::Result<Option<CodeActionResponse>> {
        let mut actions = vec![];

        let uri = params.text_document.uri;
        let Some(file_path) = self.uri_to_file_path(&uri).await else {
            return Err(tower_lsp::jsonrpc::Error::invalid_params(
                "URL argument invalid",
            ));
        };

        if let Some((_, service)) = self.get_workspace_for_file(&file_path).await {
            let port = match self.remote_preview.read().await.as_ref() {
                Some(preview) => preview.local_port,
                None => *service.port.lock().await,
            };
            let relative_path = file_path.strip_prefix(&*service.root).unwrap_or(&file_path);
            let action = CodeActionOrCommand::CodeAction(CodeAction {
                title: format!("Open in Browser({})", port),
                kind: Some(CodeActionKind::EMPTY),
                command: Some(Command {
                    title: format!("Open in Browser({})", port),
                    command: "openProjectWeb".to_string(),
                    arguments: Some(vec![
                        Value::from(service.root.to_str().unwrap_or_default().to_string()),
                        Value::from(relative_path.to_str().unwrap_or_default().to_string()),
                    ]),
                }),
                edit: None,
                diagnostics: None,
                is_preferred: Some(false),
                disabled: None,
                data: None,
            });
            actions.push(action);
        }

        Ok(Some(actions))
    }

    async fn initialized(&self, _: InitializedParams) {
        self.client
            .log_message(MessageType::INFO, "LiveServer Initialized!")
            .await;
        let folders = self.workspace_folders.lock().await;
        let mut threads = vec![];
        let remote_preview = self.remote_preview.read().await.clone();
        for (path, (name, fs)) in folders.iter() {
            if let Some(preview) = &remote_preview {
                let port = *fs.port.lock().await;
                threads.push(tokio::spawn(serve_forwarded(
                    path.clone(),
                    port,
                    fs.clone(),
                    preview.listener.clone(),
                    self.client.clone(),
                )));
                continue;
            }
            let path = path.clone();
            let f = fs.clone();
            let public = *self.public.read().await;
            threads.push(tokio::spawn(async move {
                loop {
                    let port = *f.port.clone().lock().await;
                    let _ = rusty_live_server::serve(
                        path.clone(),
                        port,
                        public,
                        Some(f.sig.clone()),
                        f.clone(),
                    )
                    .await;
                    *f.port.lock().await += 1;
                }
            }));
            self.client
                .log_message(
                    MessageType::INFO,
                    format!("Opend Workspace: {} at port {}", name, fs.port.lock().await),
                )
                .await;
        }
        *self.threads.lock().await = threads;
    }

    async fn did_close(&self, params: DidCloseTextDocumentParams) {
        let uri = params.text_document.uri;
        let Some(file_path) = self.uri_to_file_path(&uri).await else {
            return;
        };

        if let Some((_, service)) = self.get_workspace_for_file(&file_path).await {
            let mut files = service.files.lock().await;
            files.remove(&file_path);
        }
    }

    async fn shutdown(&self) -> tower_lsp::jsonrpc::Result<()> {
        self.threads.lock().await.iter().for_each(|v| v.abort());
        if let Some(preview) = self.remote_preview.read().await.as_ref() {
            preview
                .listener
                .send_replace(Some(Err("LiveServer has stopped".into())));
        }
        Ok(())
    }
}

impl Backend {
    async fn uri_to_file_path(&self, uri: &Url) -> Option<PathBuf> {
        let Ok(file_path) = uri.to_file_path() else {
            self.client
                .log_message(
                    MessageType::ERROR,
                    format!("Could not convert URI to file path: {}", uri.to_string()),
                )
                .await;
            return None;
        };
        Some(file_path)
    }

    async fn get_workspace_for_file(&self, file_path: &Path) -> Option<(PathBuf, LspFileService)> {
        let folders = self.workspace_folders.lock().await;
        let canonicalized_file = canonicalize_path(file_path);
        for (path, (_, service)) in folders.iter() {
            let canonicalized_root = canonicalize_path(&service.root);
            if canonicalized_file.starts_with(&canonicalized_root) {
                return Some((path.clone(), service.clone()));
            }
        }
        None
    }

    async fn update_file(&self, file_path: &Path, service: &LspFileService, saved: bool) {
        self.client
            .log_message(
                MessageType::INFO,
                format!("File updated: {}", file_path.display()),
            )
            .await;
        let rel = file_path
            .strip_prefix(service.root.as_ref())
            .unwrap_or(&file_path);
        self.call_custom_function(&service.root, Path::new(rel), saved)
            .await;
    }

    async fn call_custom_function(&self, workspace: &PathBuf, file_path: &Path, saved: bool) {
        if !*self.eager.read().await && !saved {
            return;
        }
        self.client
            .log_message(MessageType::INFO, format!("reload"))
            .await;
        let mutex = self.workspace_folders.lock().await;
        if let Some((_, fs)) = mutex.get(workspace) {
            fs.sig.send_signal(file_path.to_path_buf());
        }
    }
}

async fn serve_forwarded(
    root: PathBuf,
    port: u16,
    service: LspFileService,
    listener: watch::Sender<Option<Result<(), String>>>,
    client: Client,
) {
    let server = rusty_live_server::serve(root, port, false, Some(service.sig.clone()), service);
    tokio::pin!(server);

    // The pinned server binds numeric loopback before yielding in accept.
    // Recheck this when upgrading it, or switch to an explicit bind notification.
    let result = tokio::select! {
        biased;
        result = &mut server => result,
        () = std::future::ready(()) => {
            listener.send_replace(Some(Ok(())));
            server.await
        }
    };
    let message = match result {
        Err(Error::Io(error)) => format!("LiveServer failed on 127.0.0.1:{port}: {error}"),
        Ok(()) => format!("LiveServer stopped on 127.0.0.1:{port}"),
    };
    listener.send_replace(Some(Err(message.clone())));
    client.show_message(MessageType::ERROR, message).await;
}

fn remote_preview_url(port: u16, file: &Path) -> Result<Url, LspError> {
    if file.as_os_str().is_empty()
        || file
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(LspError::invalid_params(
            "Expected a workspace-relative file path",
        ));
    }

    let path = encode_url_path(file);
    Url::parse(&format!("http://127.0.0.1:{port}/{path}"))
        .map_err(|_| LspError::invalid_params("Invalid preview URL"))
}

fn preview_error(message: impl Into<std::borrow::Cow<'static, str>>) -> LspError {
    LspError {
        message: message.into(),
        ..LspError::internal_error()
    }
}

fn canonicalize_path(path: &Path) -> PathBuf {
    match path.canonicalize() {
        Ok(canonical) => canonical,
        Err(_) => {
            // If for what ever reason fails return the path as-is
            path.to_path_buf()
        }
    }
}

fn make_file_url_path(root: &Path, file_path: &Path) -> String {
    let relative_path = file_path.strip_prefix(root).unwrap_or(file_path);
    encode_url_path(relative_path)
}

fn encode_url_path(path: &Path) -> String {
    path.components()
        .filter_map(|component| match component {
            std::path::Component::Normal(value) => Some(percent_encode_path_segment(
                value.to_string_lossy().as_ref(),
            )),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn percent_encode_path_segment(segment: &str) -> String {
    let mut output = String::new();
    for byte in segment.bytes() {
        if matches!(
            byte,
            b'A'..=b'Z'
                | b'a'..=b'z'
                | b'0'..=b'9'
                | b'-'
                | b'.'
                | b'_'
                | b'~'
        ) {
            output.push(byte as char);
        } else {
            output.push_str(&format!("%{byte:02X}"));
        }
    }
    output
}

pub async fn lsp() {
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();

    let (client, server) = LspService::build(|client| Backend {
        client,
        workspace_folders: Default::default(),
        threads: Default::default(),
        port: Default::default(),
        public: Default::default(),
        remote_preview: Default::default(),
        eager: Arc::new(RwLock::new(true)),
    })
    .finish();

    Server::new(stdin, stdout, server).serve(client).await;
}

pub fn get_byte_index_from_position(s: &str, position: Position) -> usize {
    let mut line_start = 0;
    let mut lines = s.split('\n');

    for _ in 0..position.line {
        let Some(line) = lines.next() else {
            return s.len();
        };
        line_start += line.len() + 1;
    }

    let Some(line) = lines.next() else {
        return s.len();
    };
    let target = position.character as usize;
    let mut utf16_offset = 0;

    for (byte_offset, character) in line.char_indices() {
        if utf16_offset >= target {
            return line_start + byte_offset;
        }

        let next_offset = utf16_offset + character.len_utf16();
        if next_offset > target {
            return line_start + byte_offset;
        }
        utf16_offset = next_offset;
    }

    line_start + line.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_end_of_document_after_the_last_character() {
        let content = "<h1>Hello</h1>";

        assert_eq!(
            get_byte_index_from_position(content, Position::new(0, 14)),
            content.len()
        );
    }

    #[test]
    fn replaces_the_entire_document_without_retaining_its_last_character() {
        let mut content = "<h1>Hello</h1>".to_string();
        let start = get_byte_index_from_position(&content, Position::new(0, 0));
        let end = get_byte_index_from_position(&content, Position::new(0, 14));

        content.replace_range(start..end, "<!doctype html>");

        assert_eq!(content, "<!doctype html>");
    }

    #[test]
    fn maps_characters_relative_to_the_requested_line() {
        let content = "切\nabc";

        assert_eq!(
            get_byte_index_from_position(content, Position::new(1, 1)),
            5
        );
    }

    #[test]
    fn maps_utf16_positions_to_utf8_byte_offsets() {
        let content = "a😀b";

        assert_eq!(
            get_byte_index_from_position(content, Position::new(0, 3)),
            5
        );
    }

    #[test]
    fn makes_relative_url_path_for_file_under_workspace() {
        let root = Path::new("workspace");
        let file = root.join("index.html");

        assert_eq!(make_file_url_path(root, &file), "index.html");
    }

    #[test]
    fn encodes_unicode_and_reserved_characters() {
        let root = Path::new("workspace");
        let file = root.join("index_切圖_模板 #1.html");

        assert_eq!(
            make_file_url_path(root, &file),
            "index_%E5%88%87%E5%9C%96_%E6%A8%A1%E6%9D%BF%20%231.html"
        );
    }

    #[test]
    fn preserves_nested_relative_path_with_forward_slashes() {
        let root = Path::new("workspace");
        let file = root.join("Desktop").join("demo file.html");

        assert_eq!(make_file_url_path(root, &file), "Desktop/demo%20file.html");
    }

    #[test]
    fn remote_url_uses_the_forwarded_port_and_encodes_the_path() {
        let file = Path::new("examples").join("报告 #1.html");

        assert_eq!(
            remote_preview_url(57390, &file).unwrap().as_str(),
            "http://127.0.0.1:57390/examples/%E6%8A%A5%E5%91%8A%20%231.html"
        );
    }

    #[test]
    fn remote_url_requires_a_workspace_relative_file_path() {
        assert!(remote_preview_url(57390, Path::new("../index.html")).is_err());
        assert!(remote_preview_url(57390, Path::new("/index.html")).is_err());
        assert!(remote_preview_url(57390, Path::new("")).is_err());
    }
}
