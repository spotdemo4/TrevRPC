#![forbid(unsafe_code)]
#![allow(
    clippy::format_push_string,
    clippy::missing_errors_doc,
    clippy::module_name_repetitions
)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{self, Read, Write};

use heck::{ToShoutySnakeCase, ToSnakeCase, ToUpperCamelCase};
use prost::Message;
use prost_types::compiler::{CodeGeneratorRequest, CodeGeneratorResponse, code_generator_response};
use prost_types::{
    DescriptorProto, EnumDescriptorProto, FileDescriptorProto, MethodDescriptorProto,
};

/// The extension number of `(trevrpc.method)` in `google.protobuf.MethodOptions`; see
/// `proto/trevrpc/options.proto`.
const METHOD_OPTION_FIELD: u32 = 58247;

/// The marks of the `(trevrpc.method)` option of one method.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct MethodMarks {
    unreliable_requests: bool,
    unreliable_responses: bool,
    /// The `trevrpc.Compression` value, as the schema has it.
    compression: i32,
}

/// `trevrpc.Compression` values; `COMPRESSION_UNSPECIFIED` is 0.
const COMPRESSION_MESSAGE: i32 = 1;
const COMPRESSION_STREAM: i32 = 2;

/// The directions of a method that may use stream compression: those it streams, when the schema
/// sets `COMPRESSION_STREAM` and the direction's message type has no field marked `debug_redact`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct CompressedDirections {
    requests: bool,
    responses: bool,
}

/// A minimal mirror of `CodeGeneratorRequest` that keeps what `prost-types` drops.
///
/// `prost-types` decodes `google.protobuf.MethodOptions` without its extensions, so the
/// `(trevrpc.method)` option is read from the same request bytes through this mirror, which only
/// declares the path to it: `CodeGeneratorRequest.proto_file` (15) -> `FileDescriptorProto`
/// (`name` 1, `service` 6) -> `ServiceDescriptorProto` (`name` 1, `method` 2) ->
/// `MethodDescriptorProto` (`name` 1, `options` 4) -> `MethodOptions` (extension 58247) ->
/// `trevrpc.MethodOptions` (`unreliable_requests` 1, `unreliable_responses` 2, `compression` 5).
/// It also reads `FieldOptions.debug_redact` (16), which `prost-types` does not know, through
/// `FileDescriptorProto.message_type` (4) -> `DescriptorProto` (`name` 1, `field` 2,
/// `nested_type` 3) -> `FieldDescriptorProto` (`type_name` 6, `options` 8).
mod mirror {
    #[derive(Clone, PartialEq, Eq, prost::Message)]
    pub struct Request {
        #[prost(message, repeated, tag = "15")]
        pub proto_file: Vec<File>,
    }

    #[derive(Clone, PartialEq, Eq, prost::Message)]
    pub struct File {
        #[prost(string, optional, tag = "1")]
        pub name: Option<String>,
        #[prost(string, optional, tag = "2")]
        pub package: Option<String>,
        #[prost(message, repeated, tag = "4")]
        pub message_type: Vec<MessageType>,
        #[prost(message, repeated, tag = "6")]
        pub service: Vec<Service>,
    }

    #[derive(Clone, PartialEq, Eq, prost::Message)]
    pub struct MessageType {
        #[prost(string, optional, tag = "1")]
        pub name: Option<String>,
        #[prost(message, repeated, tag = "2")]
        pub field: Vec<Field>,
        #[prost(message, repeated, tag = "3")]
        pub nested_type: Vec<Self>,
    }

    #[derive(Clone, PartialEq, Eq, prost::Message)]
    pub struct Field {
        #[prost(string, optional, tag = "6")]
        pub type_name: Option<String>,
        #[prost(message, optional, tag = "8")]
        pub options: Option<FieldOptions>,
    }

    #[derive(Clone, PartialEq, Eq, prost::Message)]
    pub struct FieldOptions {
        #[prost(bool, tag = "16")]
        pub debug_redact: bool,
    }

    #[derive(Clone, PartialEq, Eq, prost::Message)]
    pub struct Service {
        #[prost(string, optional, tag = "1")]
        pub name: Option<String>,
        #[prost(message, repeated, tag = "2")]
        pub method: Vec<Method>,
    }

    #[derive(Clone, PartialEq, Eq, prost::Message)]
    pub struct Method {
        #[prost(string, optional, tag = "1")]
        pub name: Option<String>,
        #[prost(message, optional, tag = "4")]
        pub options: Option<MethodOptions>,
    }

    #[derive(Clone, PartialEq, Eq, prost::Message)]
    pub struct MethodOptions {
        #[prost(message, optional, tag = "58247")]
        pub trevrpc: Option<TrevrpcOptions>,
    }

    #[derive(Clone, PartialEq, Eq, prost::Message)]
    pub struct TrevrpcOptions {
        #[prost(bool, tag = "1")]
        pub unreliable_requests: bool,
        #[prost(bool, tag = "2")]
        pub unreliable_responses: bool,
        #[prost(int32, tag = "5")]
        pub compression: i32,
    }

    /// A `FileDescriptorSet`, with its files left encoded.
    #[derive(Clone, PartialEq, Eq, prost::Message)]
    pub struct DescriptorSet {
        #[prost(bytes = "vec", repeated, tag = "1")]
        pub file: Vec<Vec<u8>>,
    }

    /// A `CodeGeneratorRequest` assembled from encoded files.
    #[derive(Clone, PartialEq, Eq, prost::Message)]
    pub struct EncodedRequest {
        #[prost(string, repeated, tag = "1")]
        pub file_to_generate: Vec<String>,
        #[prost(string, optional, tag = "2")]
        pub parameter: Option<String>,
        #[prost(bytes = "vec", repeated, tag = "15")]
        pub proto_file: Vec<Vec<u8>>,
    }
}

/// The `(trevrpc.method)` marks of every method of a request, by file, service and method name,
/// and the message types that hold a field marked `debug_redact`.
#[derive(Debug, Default)]
struct MethodOptionIndex {
    marks: HashMap<(String, String, String), MethodMarks>,
    /// Full names, without a leading dot, of the messages that hold a redacted field, directly or
    /// in a nested message field.
    redacted: HashSet<String>,
}

impl MethodOptionIndex {
    /// Reads the marks from the encoded `CodeGeneratorRequest`.
    fn from_request_bytes(request: &[u8]) -> Result<Self, prost::DecodeError> {
        debug_assert_eq!(METHOD_OPTION_FIELD, 58247);
        let request = mirror::Request::decode(request)?;
        let mut marks = HashMap::new();
        let mut messages = HashMap::new();
        for file in &request.proto_file {
            index_messages(
                file.package.as_deref().unwrap_or_default(),
                &file.message_type,
                &mut messages,
            );
        }
        let redacted = redacted_messages(&messages);
        for file in request.proto_file {
            let file_name = file.name.unwrap_or_default();
            for service in file.service {
                let service_name = service.name.unwrap_or_default();
                for method in service.method {
                    let Some(options) = method.options.and_then(|options| options.trevrpc) else {
                        continue;
                    };
                    marks.insert(
                        (
                            file_name.clone(),
                            service_name.clone(),
                            method.name.unwrap_or_default(),
                        ),
                        MethodMarks {
                            unreliable_requests: options.unreliable_requests,
                            unreliable_responses: options.unreliable_responses,
                            compression: options.compression,
                        },
                    );
                }
            }
        }
        Ok(Self { marks, redacted })
    }

    /// Whether the message `full_name` (with or without a leading dot) holds a redacted field.
    fn is_redacted(&self, full_name: &str) -> bool {
        self.redacted.contains(full_name.trim_start_matches('.'))
    }

    fn get(&self, file: &str, service: &str, method: &str) -> MethodMarks {
        self.marks
            .get(&(file.to_owned(), service.to_owned(), method.to_owned()))
            .copied()
            .unwrap_or_default()
    }
}

/// What one message type says about redaction: whether one of its own fields is marked
/// `debug_redact`, and the message types of its fields.
#[derive(Debug, Default)]
struct MessageFields {
    redacted: bool,
    field_types: Vec<String>,
}

/// Indexes `messages` of the scope `scope` (a package, or an enclosing message's full name) and
/// their nested messages, by full name without a leading dot.
fn index_messages(
    scope: &str,
    messages: &[mirror::MessageType],
    index: &mut HashMap<String, MessageFields>,
) {
    for message in messages {
        let name = message.name.as_deref().unwrap_or_default();
        let full_name = if scope.is_empty() {
            name.to_owned()
        } else {
            format!("{scope}.{name}")
        };
        index_messages(&full_name, &message.nested_type, index);
        index.insert(
            full_name,
            MessageFields {
                redacted: message.field.iter().any(|field| {
                    field
                        .options
                        .as_ref()
                        .is_some_and(|options| options.debug_redact)
                }),
                field_types: message
                    .field
                    .iter()
                    .filter_map(|field| field.type_name.as_deref())
                    .map(|name| name.trim_start_matches('.').to_owned())
                    .collect(),
            },
        );
    }
}

/// The messages that hold a redacted field directly or through message fields, however deep.
fn redacted_messages(messages: &HashMap<String, MessageFields>) -> HashSet<String> {
    let mut redacted: HashSet<String> = messages
        .iter()
        .filter(|(_, fields)| fields.redacted)
        .map(|(name, _)| name.clone())
        .collect();
    // Spread to the messages that hold a redacted one until nothing changes; recursive types end
    // the loop because the set only grows.
    loop {
        let more: Vec<String> = messages
            .iter()
            .filter(|(name, fields)| {
                !redacted.contains(*name)
                    && fields
                        .field_types
                        .iter()
                        .any(|field| redacted.contains(field))
            })
            .map(|(name, _)| name.clone())
            .collect();
        if more.is_empty() {
            return redacted;
        }
        redacted.extend(more);
    }
}

/// Builds the encoded `CodeGeneratorRequest` for `files_to_generate` from an encoded
/// `FileDescriptorSet`.
///
/// The set must list every file, imports included, as `protox::Compiler::encode_file_descriptor_set`
/// does with imports enabled. Unlike converting through `prost_types::FileDescriptorSet`, this keeps the extension options
/// of the descriptors, such as `(trevrpc.method)`. It is meant for build scripts and tests that
/// run the generator without `protoc`.
pub fn code_generator_request(
    descriptor_set: &[u8],
    files_to_generate: &[&str],
    parameter: &str,
) -> Result<Vec<u8>, prost::DecodeError> {
    let set = mirror::DescriptorSet::decode(descriptor_set)?;
    Ok(mirror::EncodedRequest {
        file_to_generate: files_to_generate
            .iter()
            .map(|file| (*file).to_owned())
            .collect(),
        parameter: Some(parameter.to_owned()),
        proto_file: set.file,
    }
    .encode_to_vec())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PluginOptions {
    runtime_path: String,
    file_suffix: String,
    package_root: String,
    extern_paths: Vec<(String, String)>,
}

impl Default for PluginOptions {
    fn default() -> Self {
        Self {
            runtime_path: "::trevrpc".to_owned(),
            file_suffix: ".trevrpc.rs".to_owned(),
            package_root: "crate".to_owned(),
            extern_paths: Vec::new(),
        }
    }
}

/// Runs the `TrevRPC` Rust code generator using protoc plugin input and output streams.
pub fn run_plugin<R, W>(mut reader: R, mut writer: W) -> io::Result<()>
where
    R: Read,
    W: Write,
{
    let mut request_bytes = Vec::new();
    reader.read_to_end(&mut request_bytes)?;

    let request = CodeGeneratorRequest::decode(request_bytes.as_slice()).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to decode CodeGeneratorRequest: {error}"),
        )
    })?;

    let response = match MethodOptionIndex::from_request_bytes(&request_bytes) {
        Ok(index) => generate_plugin_response_with(&request, &index),
        Err(error) => generator_error(format!(
            "failed to decode the method options of CodeGeneratorRequest: {error}"
        )),
    };
    let mut output = Vec::new();
    response.encode(&mut output).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to encode CodeGeneratorResponse: {error}"),
        )
    })?;

    writer.write_all(&output)
}

#[must_use]
#[cfg(test)]
fn generate_plugin_response(request: &CodeGeneratorRequest) -> CodeGeneratorResponse {
    generate_plugin_response_with(request, &MethodOptionIndex::default())
}

#[must_use]
fn generate_plugin_response_with(
    request: &CodeGeneratorRequest,
    method_options: &MethodOptionIndex,
) -> CodeGeneratorResponse {
    let options = match parse_plugin_options(request.parameter.as_deref()) {
        Ok(options) => options,
        Err(error) => {
            return CodeGeneratorResponse {
                error: Some(error),
                supported_features: Some(proto3_optional_feature()),
                file: Vec::new(),
            };
        }
    };

    let files_to_generate = request
        .file_to_generate
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let type_index = match TypeIndex::from_files(&request.proto_file) {
        Ok(type_index) => type_index,
        Err(error) => return generator_error(error),
    };
    let mut services_by_package = BTreeMap::<String, Vec<Service>>::new();

    for file in &request.proto_file {
        let Some(file_name) = file.name.as_deref() else {
            continue;
        };

        if !files_to_generate.contains(file_name) {
            continue;
        }

        let package = file.package.clone().unwrap_or_default();
        let mut services = Vec::with_capacity(file.service.len());
        for service in &file.service {
            match descriptor_service(
                file_name,
                &package,
                service,
                &type_index,
                &options,
                method_options,
            ) {
                Ok(service) => services.push(service),
                Err(error) => {
                    return CodeGeneratorResponse {
                        error: Some(error),
                        supported_features: Some(proto3_optional_feature()),
                        file: Vec::new(),
                    };
                }
            }
        }

        services_by_package
            .entry(package)
            .or_default()
            .extend(services);
    }

    if let Err(error) = validate_generated_names(&services_by_package, &type_index) {
        return generator_error(error);
    }

    let mut output_names = HashMap::<String, String>::new();
    for (package, services) in &services_by_package {
        if services.is_empty() {
            continue;
        }
        let output_name = package_file_name(package, &options.file_suffix);
        if let Some(owner) = output_names.insert(output_name.clone(), package.clone()) {
            return generator_error(format!(
                "protobuf packages {owner:?} and {package:?} both require generated output {output_name:?}"
            ));
        }
    }

    let file = services_by_package
        .into_iter()
        .filter(|(_, services)| !services.is_empty())
        .map(|(package, services)| {
            let mut content =
                "// This file is @generated by protoc-gen-trevrpc-rust. DO NOT EDIT.\n\n"
                    .to_owned();

            for service in &services {
                generate_service(&options.runtime_path, service, &mut content);
                content.push('\n');
            }

            code_generator_response::File {
                name: Some(package_file_name(&package, &options.file_suffix)),
                insertion_point: None,
                content: Some(content),
                generated_code_info: None,
            }
        })
        .collect();

    CodeGeneratorResponse {
        error: None,
        supported_features: Some(proto3_optional_feature()),
        file,
    }
}

fn proto3_optional_feature() -> u64 {
    u64::from(i32::from(code_generator_response::Feature::Proto3Optional).unsigned_abs())
}

fn generator_error(error: String) -> CodeGeneratorResponse {
    CodeGeneratorResponse {
        error: Some(error),
        supported_features: Some(proto3_optional_feature()),
        file: Vec::new(),
    }
}

fn parse_plugin_options(parameter: Option<&str>) -> Result<PluginOptions, String> {
    let mut options = PluginOptions::default();
    let Some(parameter) = parameter else {
        return Ok(options);
    };

    for option in parameter.split(',').filter(|option| !option.is_empty()) {
        let Some((key, value)) = option.split_once('=') else {
            return Err(format!(
                "invalid trevrpc option {option:?}; expected key=value"
            ));
        };

        match key {
            "runtime_path" => value.clone_into(&mut options.runtime_path),
            "file_suffix" => value.clone_into(&mut options.file_suffix),
            "package_root" => value.clone_into(&mut options.package_root),
            "extern_path" => {
                let Some((proto_path, rust_path)) = value.split_once('=') else {
                    return Err(format!(
                        "invalid extern_path {value:?}; expected .protobuf.package=::rust::path"
                    ));
                };
                if !proto_path.starts_with('.') || proto_path == "." || rust_path.is_empty() {
                    return Err(format!(
                        "invalid extern_path {value:?}; expected .protobuf.package=::rust::path"
                    ));
                }
                if options
                    .extern_paths
                    .iter()
                    .any(|(existing, _)| existing == proto_path)
                {
                    return Err(format!("duplicate extern protobuf path {proto_path:?}"));
                }
                options
                    .extern_paths
                    .push((proto_path.to_owned(), rust_path.to_owned()));
            }
            unknown => return Err(format!("unknown trevrpc option {unknown:?}")),
        }
    }

    Ok(options)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Service {
    name: String,
    proto_name: String,
    package: String,
    methods: Vec<Method>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Method {
    name: String,
    proto_name: String,
    input_type: String,
    output_type: String,
    /// The full protobuf names of the input and output types, which pick compression
    /// dictionaries at runtime.
    input_proto_type: String,
    output_proto_type: String,
    client_streaming: bool,
    server_streaming: bool,
    unreliable: MethodMarks,
    compressed: CompressedDirections,
    /// The directions that may be compressed without a dictionary: every one, when the schema sets
    /// `COMPRESSION_MESSAGE` or `COMPRESSION_STREAM`, whose message type has no field marked
    /// `debug_redact`.
    plain: CompressedDirections,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TypeRef {
    package: String,
    rust_path: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct TypeIndex {
    messages: HashMap<String, TypeRef>,
    package_declarations: HashMap<String, HashMap<String, String>>,
    rust_declaration_paths: HashMap<String, String>,
    rust_package_paths: HashMap<String, String>,
}

impl TypeIndex {
    fn from_files(files: &[FileDescriptorProto]) -> Result<Self, String> {
        let mut index = Self::default();

        for file in files {
            let package = file.package.as_deref().unwrap_or_default();
            index.index_package_path(package)?;

            for message in &file.message_type {
                index.index_message(package, Vec::new(), Vec::new(), message)?;
            }
            for proto_enum in &file.enum_type {
                index.index_top_level_enum(package, proto_enum)?;
            }
        }

        Ok(index)
    }

    fn index_package_path(&mut self, package: &str) -> Result<(), String> {
        let rust_path = package
            .split('.')
            .filter(|module| !module.is_empty())
            .map(to_rust_module_name)
            .collect::<Vec<_>>()
            .join("::");
        if let Some(existing) = self
            .rust_package_paths
            .insert(rust_path.clone(), package.to_owned())
            && existing != package
        {
            return Err(format!(
                "protobuf packages {existing:?} and {package:?} both map to Rust module path {rust_path:?}"
            ));
        }
        Ok(())
    }

    fn index_message(
        &mut self,
        package: &str,
        mut proto_parents: Vec<String>,
        mut rust_parent_modules: Vec<String>,
        message: &DescriptorProto,
    ) -> Result<(), String> {
        let Some(name) = message.name.as_deref() else {
            return Ok(());
        };
        if message
            .options
            .as_ref()
            .and_then(|options| options.map_entry)
            .unwrap_or_default()
        {
            return Ok(());
        }
        let full_name = full_message_name(package, &proto_parents, name);
        let rust_type_name = to_rust_type_name(name);
        let rust_path = if rust_parent_modules.is_empty() {
            self.index_package_declaration(package, &rust_type_name, &full_name)?;
            rust_type_name
        } else {
            format!("{}::{rust_type_name}", rust_parent_modules.join("::"))
        };
        let type_ref = TypeRef {
            package: package.to_owned(),
            rust_path,
        };

        if let Some(existing) = self.messages.insert(full_name.clone(), type_ref.clone()) {
            return Err(format!(
                "protobuf message type {full_name:?} was described more than once ({existing:?} and {type_ref:?})"
            ));
        }
        self.index_rust_declaration_path(package, &type_ref.rust_path, &full_name)?;

        proto_parents.push(name.to_owned());
        rust_parent_modules.push(to_rust_module_name(name));

        for nested in &message.nested_type {
            self.index_message(
                package,
                proto_parents.clone(),
                rust_parent_modules.clone(),
                nested,
            )?;
        }
        for proto_enum in &message.enum_type {
            self.index_nested_enum(package, &proto_parents, &rust_parent_modules, proto_enum)?;
        }
        Ok(())
    }

    fn index_top_level_enum(
        &mut self,
        package: &str,
        proto_enum: &EnumDescriptorProto,
    ) -> Result<(), String> {
        let Some(name) = proto_enum.name.as_deref() else {
            return Ok(());
        };
        let full_name = full_message_name(package, &[], name);
        let rust_name = to_rust_type_name(name);
        self.index_package_declaration(package, &rust_name, &full_name)?;
        self.index_rust_declaration_path(package, &rust_name, &full_name)
    }

    fn index_nested_enum(
        &mut self,
        package: &str,
        proto_parents: &[String],
        rust_parent_modules: &[String],
        proto_enum: &EnumDescriptorProto,
    ) -> Result<(), String> {
        let Some(name) = proto_enum.name.as_deref() else {
            return Ok(());
        };
        let full_name = full_message_name(package, proto_parents, name);
        let rust_path = format!(
            "{}::{}",
            rust_parent_modules.join("::"),
            to_rust_type_name(name)
        );
        self.index_rust_declaration_path(package, &rust_path, &full_name)
    }

    fn index_rust_declaration_path(
        &mut self,
        package: &str,
        rust_path: &str,
        proto_name: &str,
    ) -> Result<(), String> {
        let key = format!("{package}::{rust_path}");
        if let Some(existing) = self
            .rust_declaration_paths
            .insert(key, proto_name.to_owned())
        {
            return Err(format!(
                "protobuf declarations {existing:?} and {proto_name:?} both map to Rust path {rust_path:?}"
            ));
        }
        Ok(())
    }

    fn index_package_declaration(
        &mut self,
        package: &str,
        rust_name: &str,
        proto_name: &str,
    ) -> Result<(), String> {
        let declarations = self
            .package_declarations
            .entry(package.to_owned())
            .or_default();
        if let Some(existing) = declarations.insert(rust_name.to_owned(), proto_name.to_owned()) {
            return Err(format!(
                "protobuf declarations {existing:?} and {proto_name:?} both map to Rust type {rust_name:?}"
            ));
        }
        Ok(())
    }

    fn resolve(
        &self,
        current_package: &str,
        proto_type: &str,
        options: &PluginOptions,
    ) -> Result<String, String> {
        let proto_type = self.resolve_proto_type(current_package, proto_type)?;
        let type_ref = self
            .messages
            .get(&proto_type)
            .ok_or_else(|| format!("unresolved protobuf message type {proto_type:?}"))?;

        if let Some(rust_path) = resolve_extern_path(&proto_type, &options.extern_paths)
            .or_else(|| resolve_default_well_known_type(&proto_type))
        {
            return Ok(rust_path);
        }

        if type_ref.package == current_package {
            return Ok(type_ref.rust_path.clone());
        }

        let mut path = Vec::new();
        let package_root = options.package_root.trim_end_matches("::");
        if !package_root.is_empty() {
            path.push(package_root.to_owned());
        }
        path.extend(
            type_ref
                .package
                .split('.')
                .filter(|module| !module.is_empty())
                .map(to_rust_module_name),
        );
        path.push(type_ref.rust_path.clone());
        Ok(path.join("::"))
    }

    fn resolve_proto_type(
        &self,
        current_package: &str,
        proto_type: &str,
    ) -> Result<String, String> {
        if proto_type.starts_with('.') {
            return self
                .messages
                .contains_key(proto_type)
                .then(|| proto_type.to_owned())
                .ok_or_else(|| format!("unresolved protobuf message type {proto_type:?}"));
        }

        let mut scope = current_package;
        loop {
            let candidate = if scope.is_empty() {
                format!(".{proto_type}")
            } else {
                format!(".{scope}.{proto_type}")
            };
            if self.messages.contains_key(&candidate) {
                return Ok(candidate);
            }

            let Some((parent, _)) = scope.rsplit_once('.') else {
                if scope.is_empty() {
                    break;
                }
                scope = "";
                continue;
            };
            scope = parent;
        }

        Err(format!("unresolved protobuf message type {proto_type:?}"))
    }
}

fn resolve_extern_path(proto_type: &str, extern_paths: &[(String, String)]) -> Option<String> {
    let (proto_path, rust_path) = extern_paths
        .iter()
        .filter(|(proto_path, _)| {
            proto_type == proto_path
                || proto_type
                    .strip_prefix(proto_path)
                    .is_some_and(|suffix| suffix.starts_with('.'))
        })
        .max_by_key(|(proto_path, _)| proto_path.len())?;
    if proto_type == proto_path {
        return Some(rust_path.clone());
    }

    let suffix = proto_type.strip_prefix(proto_path)?.trim_start_matches('.');
    let mut segments = suffix.split('.').collect::<Vec<_>>();
    let type_name = to_rust_type_name(segments.pop()?);
    let mut path = rust_path.trim_end_matches("::").to_owned();
    for module in segments {
        if !path.is_empty() {
            path.push_str("::");
        }
        path.push_str(&to_rust_module_name(module));
    }
    if !path.is_empty() {
        path.push_str("::");
    }
    path.push_str(&type_name);
    Some(path)
}

fn resolve_default_well_known_type(proto_type: &str) -> Option<String> {
    let rust_type = match proto_type {
        ".google.protobuf.BoolValue" => "bool",
        ".google.protobuf.BytesValue" => "::prost::alloc::vec::Vec<u8>",
        ".google.protobuf.DoubleValue" => "f64",
        ".google.protobuf.Empty" => "()",
        ".google.protobuf.FloatValue" => "f32",
        ".google.protobuf.Int32Value" => "i32",
        ".google.protobuf.Int64Value" => "i64",
        ".google.protobuf.StringValue" => "::prost::alloc::string::String",
        ".google.protobuf.UInt32Value" => "u32",
        ".google.protobuf.UInt64Value" => "u64",
        _ => {
            let name = proto_type.strip_prefix(".google.protobuf.")?;
            return Some(format!("::prost_types::{}", to_rust_type_name(name)));
        }
    };
    Some(rust_type.to_owned())
}

fn descriptor_service(
    file_name: &str,
    package: &str,
    service: &prost_types::ServiceDescriptorProto,
    type_index: &TypeIndex,
    options: &PluginOptions,
    method_options: &MethodOptionIndex,
) -> Result<Service, String> {
    let proto_name = service.name.clone().unwrap_or_default();

    let methods = service
        .method
        .iter()
        .map(|method| {
            let marks = method_options.get(
                file_name,
                &proto_name,
                method.name.as_deref().unwrap_or_default(),
            );
            descriptor_method(
                file_name,
                package,
                &proto_name,
                method,
                marks,
                type_index,
                options,
            )
            .map(|mut generated| {
                let requests = !method_options.is_redacted(&generated.input_proto_type);
                let responses = !method_options.is_redacted(&generated.output_proto_type);
                generated.compressed = CompressedDirections {
                    requests: generated.compressed.requests && requests,
                    responses: generated.compressed.responses && responses,
                };
                generated.plain = CompressedDirections {
                    requests: generated.plain.requests && requests,
                    responses: generated.plain.responses && responses,
                };
                generated
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Service {
        name: to_rust_type_name(&proto_name),
        proto_name,
        package: package.to_owned(),
        methods,
    })
}

fn descriptor_method(
    file_name: &str,
    package: &str,
    service_name: &str,
    method: &MethodDescriptorProto,
    marks: MethodMarks,
    type_index: &TypeIndex,
    options: &PluginOptions,
) -> Result<Method, String> {
    let proto_name = method.name.clone().unwrap_or_default();
    let input_proto_type = method.input_type.clone().unwrap_or_default();
    let output_proto_type = method.output_type.clone().unwrap_or_default();

    let input_type = type_index
        .resolve(package, &input_proto_type, options)
        .map_err(|error| {
            format!(
                "{file_name}: service {service_name:?}, method {proto_name:?}, input type {input_proto_type:?}: {error}"
            )
        })?;
    let output_type = type_index
        .resolve(package, &output_proto_type, options)
        .map_err(|error| {
            format!(
                "{file_name}: service {service_name:?}, method {proto_name:?}, output type {output_proto_type:?}: {error}"
            )
        })?;

    let client_streaming = method.client_streaming.unwrap_or_default();
    let server_streaming = method.server_streaming.unwrap_or_default();
    let full_name = format!(
        "{}.{proto_name}",
        qualified_service_name(package, service_name)
    );
    if marks.unreliable_requests && !client_streaming {
        return Err(format!(
            "{full_name}: unreliable_requests requires a client-streaming or bidirectional method"
        ));
    }
    if marks.unreliable_responses && !server_streaming {
        return Err(format!(
            "{full_name}: unreliable_responses requires a server-streaming or bidirectional method"
        ));
    }
    // COMPRESSION_STREAM includes COMPRESSION_MESSAGE, for the messages outside the stream.
    let (stream, plain) = match marks.compression {
        0 => (false, false),
        COMPRESSION_MESSAGE => (false, true),
        COMPRESSION_STREAM => (true, true),
        other => return Err(format!("{full_name}: unknown compression {other}")),
    };
    if stream && !client_streaming && !server_streaming {
        return Err(format!(
            "{full_name}: COMPRESSION_STREAM requires a streaming method"
        ));
    }

    Ok(Method {
        name: to_rust_method_name(&proto_name),
        proto_name,
        input_type,
        output_type,
        input_proto_type: input_proto_type.trim_start_matches('.').to_owned(),
        output_proto_type: output_proto_type.trim_start_matches('.').to_owned(),
        client_streaming,
        server_streaming,
        unreliable: marks,
        compressed: CompressedDirections {
            requests: stream && client_streaming,
            responses: stream && server_streaming,
        },
        plain: CompressedDirections {
            requests: plain,
            responses: plain,
        },
    })
}

/// The service name qualified by its package: `package.Service`, or `Service` without a package.
fn qualified_service_name(package: &str, service: &str) -> String {
    if package.is_empty() {
        service.to_owned()
    } else {
        format!("{package}.{service}")
    }
}

fn validate_generated_names(
    services_by_package: &BTreeMap<String, Vec<Service>>,
    type_index: &TypeIndex,
) -> Result<(), String> {
    for (package, services) in services_by_package {
        let mut type_owners = type_index
            .package_declarations
            .get(package)
            .cloned()
            .unwrap_or_default();
        let mut registration_owners = HashMap::<String, String>::new();

        for service in services {
            let service_path = service_path(service);
            for declaration in [
                service.name.clone(),
                format!("{}Client", service.name),
                paths_module_name(service),
            ] {
                let key = semantic_rust_identifier(&declaration).to_owned();
                if let Some(owner) = type_owners.insert(key.clone(), service_path.clone()) {
                    return Err(format!(
                        "protobuf declarations {owner:?} and {service_path:?} both require generated Rust type {key:?}"
                    ));
                }
            }

            let registration = format!("register_{}", to_snake_case(&service.name));
            let registration_key = semantic_rust_identifier(&registration).to_owned();
            if let Some(owner) =
                registration_owners.insert(registration_key.clone(), service_path.clone())
            {
                return Err(format!(
                    "protobuf services {owner:?} and {service_path:?} both require generated Rust function {registration_key:?}"
                ));
            }

            validate_generated_method_names(service)?;
        }
    }
    Ok(())
}

fn validate_generated_method_names(service: &Service) -> Result<(), String> {
    let mut owners = HashMap::<String, String>::new();
    for fixed in [
        "new",
        "with_default_call_options",
        "default_call_options",
        "transport",
        "into_transport",
    ] {
        owners.insert(fixed.to_owned(), "generated client support".to_owned());
    }

    for method in &service.methods {
        for generated_name in generated_client_method_names(method) {
            let key = semantic_rust_identifier(&generated_name).to_owned();
            if let Some(owner) = owners.insert(key.clone(), method.proto_name.clone()) {
                return Err(format!(
                    "service {:?} RPC methods {owner:?} and {:?} both require generated Rust method {key:?}",
                    service_path(service),
                    method.proto_name
                ));
            }
        }
    }
    Ok(())
}

fn generated_client_method_names(method: &Method) -> Vec<String> {
    vec![method.name.clone()]
}

fn semantic_rust_identifier(identifier: &str) -> &str {
    identifier.strip_prefix("r#").unwrap_or(identifier)
}

/// Returns the `Unreliable` constant for the directions a method marks, or `None` when it marks
/// neither.
fn unreliable_expr(runtime_path: &str, method: &Method) -> Option<String> {
    let constant = match (
        method.unreliable.unreliable_requests,
        method.unreliable.unreliable_responses,
    ) {
        (false, false) => return None,
        (true, false) => "REQUESTS",
        (false, true) => "RESPONSES",
        (true, true) => "BOTH",
    };
    Some(format!("{runtime_path}::Unreliable::{constant}"))
}

/// Returns the constant of type `kind` (`CompressStream` or `CompressMessages`) for `directions`,
/// or `None` when neither is set.
fn directions_expr(
    runtime_path: &str,
    kind: &str,
    directions: CompressedDirections,
) -> Option<String> {
    let constant = match (directions.requests, directions.responses) {
        (false, false) => return None,
        (true, false) => "REQUESTS",
        (false, true) => "RESPONSES",
        (true, true) => "BOTH",
    };
    Some(format!("{runtime_path}::{kind}::{constant}"))
}

/// Returns the `CompressStream` constant for the directions of a method that may use stream
/// compression, or `None` when neither may.
fn compress_stream_expr(runtime_path: &str, method: &Method) -> Option<String> {
    directions_expr(runtime_path, "CompressStream", method.compressed)
}

/// Returns the `CompressMessages` constant for the directions of a method that may be compressed
/// without a dictionary, or `None` when neither may.
fn compress_expr(runtime_path: &str, method: &Method) -> Option<String> {
    directions_expr(runtime_path, "CompressMessages", method.plain)
}

/// Returns the doc lines that say which directions of a method travel as datagrams when they can.
fn unreliable_docs(method: &Method) -> String {
    let directions = match (
        method.unreliable.unreliable_requests,
        method.unreliable.unreliable_responses,
    ) {
        (false, false) => return String::new(),
        (true, false) => "request",
        (false, true) => "response",
        (true, true) => "request and response",
    };
    format!(
        "    ///\n    /// The schema marks the {directions} messages of this method as unreliable: where the\n    /// connection has datagrams they may be lost or reordered, but never duplicated.\n"
    )
}

fn generate_service(runtime_path: &str, service: &Service, buf: &mut String) {
    generate_paths(service, buf);
    generate_trait(runtime_path, service, buf);
    generate_client(runtime_path, service, buf);
    generate_registration(runtime_path, service, buf);
}

/// Returns the module holding the method path constants of a service.
fn paths_module_name(service: &Service) -> String {
    format!("{}_paths", to_snake_case(&service.name))
}

/// Returns the name of the constant holding the path of a method.
fn path_const_name(method: &Method) -> String {
    method.proto_name.to_shouty_snake_case()
}

/// Returns the expression naming the path constant of a method.
fn path_const_expr(service: &Service, method: &Method) -> String {
    format!(
        "{}::{}",
        paths_module_name(service),
        path_const_name(method)
    )
}

/// Returns the wire path of a method, `/<package>.<Service>/<Method>`.
fn method_path(service: &Service, method: &Method) -> String {
    format!("/{}/{}", service_path(service), method.proto_name)
}

fn generate_paths(service: &Service, buf: &mut String) {
    buf.push_str(&format!(
        "/// Method paths of the `{}` service, as sent on the wire.\npub mod {} {{\n",
        service_path(service),
        paths_module_name(service)
    ));
    for method in &service.methods {
        buf.push_str(&format!(
            "    /// Path of the `{}` RPC.\n    pub const {}: &str = {:?};\n",
            method.proto_name,
            path_const_name(method),
            method_path(service, method)
        ));
    }
    buf.push_str("}\n\n");
}

fn generate_trait(runtime_path: &str, service: &Service, buf: &mut String) {
    buf.push_str(&format!(
        "#[allow(clippy::missing_errors_doc)]\n#[{runtime_path}::async_trait]\npub trait {}: Send + Sync + 'static {{\n",
        service.name
    ));

    for method in &service.methods {
        buf.push_str(&format!(
            "    /// Handles the `{}` RPC.\n{}",
            method.proto_name,
            unreliable_docs(method)
        ));
        if method.client_streaming && method.server_streaming {
            buf.push_str(&format!(
                "    async fn {}(&self, context: {runtime_path}::server::RequestContext, requests: {runtime_path}::BoxStream<{}>) -> ::core::result::Result<{runtime_path}::BoxStream<{}>, {runtime_path}::Status>;\n",
                method.name, method.input_type, method.output_type
            ));
        } else if method.client_streaming {
            buf.push_str(&format!(
                "    async fn {}(&self, context: {runtime_path}::server::RequestContext, requests: {runtime_path}::BoxStream<{}>) -> ::core::result::Result<{}, {runtime_path}::Status>;\n",
                method.name, method.input_type, method.output_type
            ));
        } else if method.server_streaming {
            buf.push_str(&format!(
                "    async fn {}(&self, context: {runtime_path}::server::RequestContext, request: {}) -> ::core::result::Result<{runtime_path}::BoxStream<{}>, {runtime_path}::Status>;\n",
                method.name, method.input_type, method.output_type
            ));
        } else {
            buf.push_str(&format!(
                "    async fn {}(&self, context: {runtime_path}::server::RequestContext, request: {}) -> ::core::result::Result<{}, {runtime_path}::Status>;\n",
                method.name, method.input_type, method.output_type
            ));
        }
    }

    buf.push_str("}\n\n");
}

fn generate_client(runtime_path: &str, service: &Service, buf: &mut String) {
    let client_name = format!("{}Client", service.name);

    buf.push_str(&format!(
        "#[derive(Clone)]\npub struct {client_name}<T> {{\n    transport: T,\n    default_options: {runtime_path}::client::CallOptions,\n}}\n\n"
    ));

    buf.push_str(&format!(
        "impl<T> {client_name}<T> {{\n    /// Creates a client backed by the provided transport.\n    #[must_use]\n    pub fn new(transport: T) -> Self {{\n        Self {{ transport, default_options: {runtime_path}::client::CallOptions::new() }}\n    }}\n\n    /// Creates a client with default call options applied to every RPC.\n    #[must_use]\n    pub const fn with_default_call_options(transport: T, default_options: {runtime_path}::client::CallOptions) -> Self {{\n        Self {{ transport, default_options }}\n    }}\n\n    /// Returns the default call options.\n    #[must_use]\n    pub const fn default_call_options(&self) -> &{runtime_path}::client::CallOptions {{\n        &self.default_options\n    }}\n\n    /// Returns the underlying transport.\n    #[must_use]\n    pub const fn transport(&self) -> &T {{\n        &self.transport\n    }}\n\n    /// Consumes the client and returns the underlying transport.\n    #[must_use]\n    pub fn into_transport(self) -> T {{\n        self.transport\n    }}\n}}\n\n"
    ));

    buf.push_str(&format!(
        "impl<T> {client_name}<T>\nwhere\n    T: {runtime_path}::client::RpcTransport,\n{{\n"
    ));

    for method in service
        .methods
        .iter()
        .filter(|method| !method.client_streaming && !method.server_streaming)
    {
        generate_client_method(runtime_path, service, method, buf);
    }

    buf.push_str("}\n\n");
    buf.push_str(&format!(
        "impl<T> {client_name}<T>\nwhere\n    T: {runtime_path}::client::StreamingRpcTransport,\n{{\n"
    ));

    for method in service
        .methods
        .iter()
        .filter(|method| method.client_streaming || method.server_streaming)
    {
        generate_client_method(runtime_path, service, method, buf);
    }

    buf.push_str("}\n\n");
}

/// Emits the client method of `method`, which returns the call to make: awaiting it makes the
/// call, and its builder methods change the call's options or shape first.
fn generate_client_method(
    runtime_path: &str,
    service: &Service,
    method: &Method,
    buf: &mut String,
) {
    let path = path_const_expr(service, method);
    // The schema's marks are the call's default, so the client's default options and the call's
    // own options override them.
    let marks = unreliable_expr(runtime_path, method)
        .map(|unreliable| format!(".default_unreliable({unreliable})"))
        .unwrap_or_default();
    let compress_stream = compress_stream_expr(runtime_path, method)
        .map(|directions| format!(".compress_stream({directions})"))
        .unwrap_or_default();
    let compress = compress_expr(runtime_path, method)
        .map(|directions| format!(".compress({directions})"))
        .unwrap_or_default();
    let marks = format!(
        ".message_types({:?}, {:?}){marks}{compress_stream}{compress}",
        method.input_proto_type, method.output_proto_type
    );
    let (shape, constructor, request_param, request_arg) =
        match (method.client_streaming, method.server_streaming) {
            (true, true) => (
                "BidirectionalStreaming",
                "bidirectional_streaming",
                String::new(),
                "",
            ),
            (true, false) => ("ClientStreaming", "client_streaming", String::new(), ""),
            (false, true) => (
                "ServerStreaming",
                "server_streaming",
                format!(", request: &{}", method.input_type),
                " request,",
            ),
            (false, false) => (
                "Unary",
                "unary",
                format!(", request: &{}", method.input_type),
                " request,",
            ),
        };
    buf.push_str(&format!(
        "    /// Calls the `{}` RPC when the returned call is awaited.\n{}    pub fn {}(&self{request_param}) -> {runtime_path}::client::Call<'_, T, {runtime_path}::client::call::{shape}<{}, {}>> {{\n        {runtime_path}::client::{constructor}(&self.transport, {path},{request_arg} self.default_options.clone()){marks}\n    }}\n\n",
        method.proto_name,
        unreliable_docs(method),
        method.name,
        method.input_type,
        method.output_type,
    ));
}

fn generate_registration(runtime_path: &str, service: &Service, buf: &mut String) {
    let register_name = format!("register_{}", to_snake_case(&service.name));
    let service_name = service_path(service);

    buf.push_str(&format!(
        "/// Registers routes for the `{service_name}` service.\npub fn {register_name}<S>(server: &mut {runtime_path}::server::Server, service: S)\nwhere\n    S: {},\n{{\n    let service = ::std::sync::Arc::new(service);\n",
        service.name
    ));

    for method in &service.methods {
        generate_registration_method(runtime_path, service, method, buf);
    }

    buf.push_str("}\n");
}

fn generate_registration_method(
    runtime_path: &str,
    service: &Service,
    method: &Method,
    buf: &mut String,
) {
    let start = buf.len();
    emit_registration_method(runtime_path, service, method, buf);
    // Routes register through the options variants of `route_with_context` and
    // `route_streaming_with_context`, which take the route's message types, and its unreliable
    // marks, as one more argument before the handler.
    let mut options = format!(
        "{runtime_path}::server::RouteOptions::new().with_message_types({:?}, {:?})",
        method.input_proto_type, method.output_proto_type
    );
    if let Some(unreliable) = unreliable_expr(runtime_path, method) {
        options.push_str(&format!(".with_unreliable({unreliable})"));
    }
    if let Some(directions) = compress_stream_expr(runtime_path, method) {
        options.push_str(&format!(".with_compress_stream({directions})"));
    }
    if let Some(directions) = compress_expr(runtime_path, method) {
        options.push_str(&format!(".with_compress({directions})"));
    }
    let emitted = buf.split_off(start);
    buf.push_str(
        &emitted
            .replace(
                "route_streaming_with_context(",
                "route_streaming_with_options(",
            )
            .replace("route_with_context(", "route_with_options(")
            .replace(", move |context,", &format!(", {options}, move |context,")),
    );
}

fn emit_registration_method(
    runtime_path: &str,
    service: &Service,
    method: &Method,
    buf: &mut String,
) {
    let path = path_const_expr(service, method);
    if method.client_streaming && method.server_streaming {
        buf.push_str(&format!(
            "\n    {{\n        let service = ::std::sync::Arc::clone(&service);\n        server.route_streaming_with_context({path}, {runtime_path}::RpcKind::BidirectionalStreaming, move |context, _body, request_stream| {{\n            let service = ::std::sync::Arc::clone(&service);\n            async move {{\n                let requests = {runtime_path}::stream::decode::<{}>(request_stream);\n                let responses = service.{}(context, requests).await?;\n                Ok({runtime_path}::stream::encode(responses))\n            }}\n        }});\n    }}\n",
            method.input_type, method.name
        ));
    } else if method.client_streaming {
        buf.push_str(&format!(
            "\n    {{\n        let service = ::std::sync::Arc::clone(&service);\n        server.route_streaming_with_context({path}, {runtime_path}::RpcKind::ClientStreaming, move |context, _body, request_stream| {{\n            let service = ::std::sync::Arc::clone(&service);\n            async move {{\n                let requests = {runtime_path}::stream::decode::<{}>(request_stream);\n                let response = service.{}(context, requests).await?;\n                Ok({runtime_path}::stream::encode({runtime_path}::stream::from_iter([response])))\n            }}\n        }});\n    }}\n",
            method.input_type, method.name
        ));
    } else if method.server_streaming {
        buf.push_str(&format!(
            "\n    {{\n        let service = ::std::sync::Arc::clone(&service);\n        server.route_streaming_with_context({path}, {runtime_path}::RpcKind::ServerStreaming, move |context, body, _request_stream| {{\n            let service = ::std::sync::Arc::clone(&service);\n            async move {{\n                let request = <{} as ::prost::Message>::decode(body.as_slice())?;\n                let responses = service.{}(context, request).await?;\n                Ok({runtime_path}::stream::encode(responses))\n            }}\n        }});\n    }}\n",
            method.input_type, method.name
        ));
    } else {
        buf.push_str(&format!(
            "\n    {{\n        let service = ::std::sync::Arc::clone(&service);\n        server.route_with_context({path}, move |context, body| {{\n            let service = ::std::sync::Arc::clone(&service);\n            async move {{\n                let request = <{} as ::prost::Message>::decode(body.as_slice())?;\n                let response = service.{}(context, request).await?;\n                Ok(::prost::Message::encode_to_vec(&response))\n            }}\n        }});\n    }}\n",
            method.input_type, method.name
        ));
    }
}

fn service_path(service: &Service) -> String {
    qualified_service_name(&service.package, &service.proto_name)
}

fn package_file_name(package: &str, suffix: &str) -> String {
    if package.is_empty() {
        format!("_{suffix}")
    } else {
        format!("{package}{suffix}")
    }
}

fn full_message_name(package: &str, parents: &[String], name: &str) -> String {
    let mut parts = Vec::new();

    if !package.is_empty() {
        parts.push(package);
    }

    parts.extend(parents.iter().map(String::as_str));
    parts.push(name);

    format!(".{}", parts.join("."))
}

fn to_rust_type_name(input: &str) -> String {
    to_rust_ident(&input.to_upper_camel_case())
}

fn to_rust_method_name(input: &str) -> String {
    to_rust_ident(&input.to_snake_case())
}

fn to_rust_module_name(input: &str) -> String {
    to_rust_ident(&input.to_snake_case())
}

fn to_rust_ident(input: &str) -> String {
    if RUST_RAW_KEYWORDS.contains(&input) {
        format!("r#{input}")
    } else if RUST_SUFFIXED_KEYWORDS.contains(&input) {
        format!("{input}_")
    } else if input.starts_with(char::is_numeric) {
        format!("_{input}")
    } else {
        input.to_owned()
    }
}

const RUST_RAW_KEYWORDS: &[&str] = &[
    "abstract", "as", "async", "await", "become", "box", "break", "const", "continue", "do", "dyn",
    "else", "enum", "false", "final", "fn", "for", "gen", "if", "impl", "in", "let", "loop",
    "macro", "match", "mod", "move", "mut", "override", "priv", "pub", "ref", "return", "static",
    "struct", "trait", "true", "try", "type", "typeof", "unsafe", "unsized", "use", "virtual",
    "where", "while", "yield",
];
const RUST_SUFFIXED_KEYWORDS: &[&str] = &["_", "Self", "crate", "extern", "self", "super"];

fn to_snake_case(input: &str) -> String {
    let mut output = String::with_capacity(input.len());

    for (index, character) in input.chars().enumerate() {
        if character.is_uppercase() {
            if index > 0 {
                output.push('_');
            }
            output.extend(character.to_lowercase());
        } else {
            output.push(character);
        }
    }

    output
}

#[cfg(test)]
mod tests {
    use prost::Message;
    use prost_types::compiler::CodeGeneratorRequest;
    use prost_types::{
        DescriptorProto, FileDescriptorProto, MessageOptions, MethodDescriptorProto,
    };

    use super::{
        Service, generate_plugin_response, generate_service, package_file_name, service_path,
        to_rust_method_name, to_snake_case,
    };

    use super::{CompressedDirections, Method, MethodMarks};

    #[test]
    fn builds_proto_service_paths() {
        let service = Service {
            name: "Greeter".to_owned(),
            proto_name: "Greeter".to_owned(),
            package: "hello.v1".to_owned(),
            methods: Vec::new(),
        };

        assert_eq!(service_path(&service), "hello.v1.Greeter");
    }

    #[test]
    fn converts_service_names_to_register_function_names() {
        assert_eq!(to_snake_case("Greeter"), "greeter");
        assert_eq!(to_snake_case("ChatService"), "chat_service");
    }

    #[test]
    fn builds_plugin_file_names() {
        assert_eq!(
            package_file_name("hello.v1", ".trevrpc.rs"),
            "hello.v1.trevrpc.rs"
        );
        assert_eq!(package_file_name("", ".trevrpc.rs"), "_.trevrpc.rs");
    }

    #[test]
    fn escapes_rust_method_keywords() {
        assert_eq!(to_rust_method_name("Type"), "r#type");
    }

    #[test]
    fn generates_unary_service_api() {
        let service = Service {
            name: "Greeter".to_owned(),
            proto_name: "Greeter".to_owned(),
            package: "hello.v1".to_owned(),
            methods: vec![Method {
                name: "say_hello".to_owned(),
                proto_name: "SayHello".to_owned(),
                input_type: "HelloRequest".to_owned(),
                output_type: "HelloReply".to_owned(),
                input_proto_type: "example.HelloRequest".to_owned(),
                output_proto_type: "example.HelloReply".to_owned(),
                client_streaming: false,
                server_streaming: false,
                unreliable: MethodMarks::default(),
                compressed: CompressedDirections::default(),
                plain: CompressedDirections::default(),
            }],
        };
        let mut generated = String::new();

        generate_service("::trevrpc", &service, &mut generated);

        assert!(generated.contains("pub trait Greeter"));
        assert!(generated.contains("pub struct GreeterClient<T>"));
        assert!(generated.contains("pub fn register_greeter<S>"));
        assert!(generated.contains("hello.v1.Greeter"));
        assert!(generated.contains("SayHello"));
    }

    #[test]
    fn generates_method_path_constants_used_by_clients_and_registration() {
        let service = Service {
            name: "Greeter".to_owned(),
            proto_name: "Greeter".to_owned(),
            package: "hello.v1".to_owned(),
            methods: vec![Method {
                name: "say_hello".to_owned(),
                proto_name: "SayHello".to_owned(),
                input_type: "HelloRequest".to_owned(),
                output_type: "HelloReply".to_owned(),
                input_proto_type: "example.HelloRequest".to_owned(),
                output_proto_type: "example.HelloReply".to_owned(),
                client_streaming: false,
                server_streaming: false,
                unreliable: MethodMarks::default(),
                compressed: CompressedDirections::default(),
                plain: CompressedDirections::default(),
            }],
        };
        let mut generated = String::new();

        generate_service("::trevrpc", &service, &mut generated);

        assert!(generated.contains("pub mod greeter_paths {"));
        assert!(generated.contains("pub const SAY_HELLO: &str = \"/hello.v1.Greeter/SayHello\";"));
        assert!(generated.contains(
            "::trevrpc::client::unary(&self.transport, greeter_paths::SAY_HELLO, request, self.default_options.clone()).message_types(\"example.HelloRequest\", \"example.HelloReply\")"
        ));
        // One client method per RPC: the returned call carries the options and response variants.
        assert!(generated.contains(
            "pub fn say_hello(&self, request: &HelloRequest) -> ::trevrpc::client::Call<'_, T, ::trevrpc::client::call::Unary<HelloRequest, HelloReply>>"
        ));
        for variant in ["_with_options", "_envelope", "_from_stream"] {
            assert!(
                !generated.contains(&format!("fn say_hello{variant}")),
                "generated {variant} variant"
            );
        }
        // Routes carry their message types, which pick compression dictionaries.
        assert!(generated.contains(
            "server.route_with_options(greeter_paths::SAY_HELLO, ::trevrpc::server::RouteOptions::new().with_message_types(\"example.HelloRequest\", \"example.HelloReply\"), move |context,"
        ));
        // Handlers return plain messages and set response metadata through their context.
        assert!(generated.contains(
            "request: HelloRequest) -> ::core::result::Result<HelloReply, ::trevrpc::Status>;"
        ));
        assert!(!generated.contains("SERVICE"));
    }

    #[test]
    fn method_paths_omit_the_package_when_there_is_none() {
        let service = Service {
            name: "Greeter".to_owned(),
            proto_name: "Greeter".to_owned(),
            package: String::new(),
            methods: vec![Method {
                name: "say_hello".to_owned(),
                proto_name: "SayHello".to_owned(),
                input_type: "HelloRequest".to_owned(),
                output_type: "HelloReply".to_owned(),
                input_proto_type: "example.HelloRequest".to_owned(),
                output_proto_type: "example.HelloReply".to_owned(),
                client_streaming: false,
                server_streaming: false,
                unreliable: MethodMarks::default(),
                compressed: CompressedDirections::default(),
                plain: CompressedDirections::default(),
            }],
        };
        let mut generated = String::new();

        generate_service("::trevrpc", &service, &mut generated);

        assert!(generated.contains("pub const SAY_HELLO: &str = \"/Greeter/SayHello\";"));
    }

    #[test]
    fn generates_streaming_service_api() {
        let service = Service {
            name: "Greeter".to_owned(),
            proto_name: "Greeter".to_owned(),
            package: "hello.v1".to_owned(),
            methods: vec![
                Method {
                    name: "lots_of_replies".to_owned(),
                    proto_name: "LotsOfReplies".to_owned(),
                    input_type: "HelloRequest".to_owned(),
                    output_type: "HelloReply".to_owned(),
                    input_proto_type: "example.HelloRequest".to_owned(),
                    output_proto_type: "example.HelloReply".to_owned(),
                    client_streaming: false,
                    server_streaming: true,
                    unreliable: MethodMarks::default(),
                    compressed: CompressedDirections::default(),
                    plain: CompressedDirections::default(),
                },
                Method {
                    name: "lots_of_greetings".to_owned(),
                    proto_name: "LotsOfGreetings".to_owned(),
                    input_type: "HelloRequest".to_owned(),
                    output_type: "HelloReply".to_owned(),
                    input_proto_type: "example.HelloRequest".to_owned(),
                    output_proto_type: "example.HelloReply".to_owned(),
                    client_streaming: true,
                    server_streaming: false,
                    unreliable: MethodMarks::default(),
                    compressed: CompressedDirections::default(),
                    plain: CompressedDirections::default(),
                },
                Method {
                    name: "bidi_hello".to_owned(),
                    proto_name: "BidiHello".to_owned(),
                    input_type: "HelloRequest".to_owned(),
                    output_type: "HelloReply".to_owned(),
                    input_proto_type: "example.HelloRequest".to_owned(),
                    output_proto_type: "example.HelloReply".to_owned(),
                    client_streaming: true,
                    server_streaming: true,
                    unreliable: MethodMarks::default(),
                    compressed: CompressedDirections::default(),
                    plain: CompressedDirections::default(),
                },
            ],
        };
        let mut generated = String::new();

        generate_service("::trevrpc", &service, &mut generated);

        assert!(generated.contains("BoxStream<HelloRequest>"));
        assert!(generated.contains("BoxStream<HelloReply>"));
        assert!(generated.contains("::trevrpc::client::server_streaming"));
        assert!(generated.contains("::trevrpc::client::client_streaming"));
        assert!(
            generated
                .contains("::trevrpc::client::call::ClientStreaming<HelloRequest, HelloReply>")
        );
        assert!(generated.contains("::trevrpc::client::bidirectional_streaming"));
        assert!(
            generated.contains(
                "::trevrpc::client::call::BidirectionalStreaming<HelloRequest, HelloReply>"
            )
        );
        assert!(generated.contains("server.route_streaming"));
        assert!(generated.contains("greeter_paths::LOTS_OF_REPLIES,"));
        assert!(generated.contains("greeter_paths::LOTS_OF_GREETINGS,"));
        assert!(generated.contains("greeter_paths::BIDI_HELLO,"));
        assert!(generated.contains("::trevrpc::RpcKind::ServerStreaming"));
        assert!(generated.contains("::trevrpc::RpcKind::ClientStreaming"));
        assert!(generated.contains("::trevrpc::RpcKind::BidirectionalStreaming"));
        assert!(generated.contains("::trevrpc::stream::decode::<HelloRequest>"));
        assert!(generated.contains("::trevrpc::stream::encode"));
    }

    #[test]
    fn plugin_generates_package_service_file() {
        let request = hello_request(None);

        let response = generate_plugin_response(&request);

        assert_eq!(response.error, None);
        assert_eq!(response.file.len(), 1);
        assert_eq!(
            response.file[0].name.as_deref(),
            Some("hello.v1.trevrpc.rs")
        );

        let content = response.file[0]
            .content
            .as_deref()
            .expect("plugin should generate file content");

        assert!(content.contains("pub trait Greeter"));
        assert!(content.contains("pub struct GreeterClient<T>"));
        assert!(content.contains("pub fn register_greeter<S>"));
        assert!(content.contains("async fn say_hello"));
        assert!(content.contains("options: ::trevrpc::client::CallOptions"));
        assert!(content.contains("HelloRequest"));
        assert!(content.contains("HelloReply"));
    }

    #[test]
    fn plugin_accepts_generation_options() {
        let request = hello_request(Some(
            "runtime_path=::my_rpc,file_suffix=.rpc.rs,package_root=crate::pb".to_owned(),
        ));

        let response = generate_plugin_response(&request);

        assert_eq!(response.error, None);
        assert_eq!(response.file[0].name.as_deref(), Some("hello.v1.rpc.rs"));
        assert!(
            response.file[0]
                .content
                .as_deref()
                .expect("plugin should generate file content")
                .contains("::my_rpc::client::RpcTransport")
        );
    }

    #[test]
    fn plugin_uses_prost_default_well_known_type_paths() {
        let mut request = hello_request(None);
        request.proto_file.push(FileDescriptorProto {
            name: Some("google/protobuf/empty.proto".to_owned()),
            package: Some("google.protobuf".to_owned()),
            message_type: vec![message("Empty")],
            ..Default::default()
        });
        request.proto_file[0].service[0].method[0].input_type =
            Some(".google.protobuf.Empty".to_owned());
        request.proto_file[0].service[0].method[0].output_type =
            Some(".google.protobuf.Empty".to_owned());

        let response = generate_plugin_response(&request);

        assert_eq!(response.error, None);
        let content = response.file[0]
            .content
            .as_deref()
            .expect("plugin should generate service content");
        assert!(content.contains("request: ()"));
        assert!(content.contains("-> ::core::result::Result<(), ::trevrpc::Status>;"));
        assert!(!content.contains("crate::google::protobuf::Empty"));
    }

    #[test]
    fn plugin_resolves_custom_extern_paths() {
        let mut request = hello_request(Some("extern_path=.shared=::external::pb".to_owned()));
        request.proto_file.push(FileDescriptorProto {
            name: Some("proto/shared.proto".to_owned()),
            package: Some("shared".to_owned()),
            message_type: vec![message("ExternalRequest")],
            ..Default::default()
        });
        request.proto_file[0].service[0].method[0].input_type =
            Some(".shared.ExternalRequest".to_owned());

        let response = generate_plugin_response(&request);

        assert_eq!(response.error, None);
        let content = response.file[0]
            .content
            .as_deref()
            .expect("plugin should generate service content");
        assert!(content.contains("request: ::external::pb::ExternalRequest"));
    }

    #[test]
    fn plugin_resolves_relative_types_through_parent_packages() {
        let mut request = hello_request(None);
        request.proto_file.push(FileDescriptorProto {
            name: Some("proto/parent.proto".to_owned()),
            package: Some("hello".to_owned()),
            message_type: vec![message("ParentRequest")],
            ..Default::default()
        });
        request.proto_file[0].service[0].method[0].input_type = Some("ParentRequest".to_owned());

        let response = generate_plugin_response(&request);

        assert_eq!(response.error, None);
        let content = response.file[0]
            .content
            .as_deref()
            .expect("plugin should generate service content");
        assert!(content.contains("request: crate::hello::ParentRequest"));
    }

    #[test]
    fn plugin_reports_bad_options_in_response() {
        let request = hello_request(Some("bad".to_owned()));
        let response = generate_plugin_response(&request);

        assert!(response.error.is_some());
        assert!(response.file.is_empty());
    }

    #[test]
    fn plugin_rejects_packages_with_the_same_rust_module_path() {
        let mut request = hello_request(None);
        request.proto_file.push(FileDescriptorProto {
            name: Some("proto/collision.proto".to_owned()),
            package: Some("hello_v1".to_owned()),
            message_type: vec![message("Other")],
            ..Default::default()
        });
        request.proto_file[0].package = Some("helloV1".to_owned());
        request.proto_file[0].service[0].method[0].input_type =
            Some(".helloV1.HelloRequest".to_owned());
        request.proto_file[0].service[0].method[0].output_type =
            Some(".helloV1.HelloReply".to_owned());

        let response = generate_plugin_response(&request);

        let error = response
            .error
            .expect("colliding Rust package modules should fail generation");
        assert!(error.contains("protobuf packages \"helloV1\" and \"hello_v1\""));
        assert!(error.contains("Rust module path \"hello_v1\""));
        assert!(response.file.is_empty());
    }

    #[test]
    fn plugin_rejects_unresolved_input_descriptor() {
        let mut request = hello_request(None);
        request.proto_file[0].service[0].method[0].input_type =
            Some(".hello.v1.MissingRequest".to_owned());

        let response = generate_plugin_response(&request);

        let error = response
            .error
            .expect("unresolved input should fail generation");
        assert!(error.contains("input type \".hello.v1.MissingRequest\""));
        assert!(error.contains("unresolved protobuf message type"));
        assert!(response.file.is_empty());
    }

    #[test]
    fn plugin_rejects_unresolved_output_descriptor() {
        let mut request = hello_request(None);
        request.proto_file[0].service[0].method[0].output_type =
            Some(".hello.v1.MissingReply".to_owned());

        let response = generate_plugin_response(&request);

        let error = response
            .error
            .expect("unresolved output should fail generation");
        assert!(error.contains("output type \".hello.v1.MissingReply\""));
        assert!(error.contains("unresolved protobuf message type"));
        assert!(response.file.is_empty());
    }

    #[test]
    fn plugin_rejects_nested_messages_with_the_same_rust_path() {
        let mut request = hello_request(None);
        request.proto_file[0].message_type = vec![DescriptorProto {
            name: Some("Outer".to_owned()),
            nested_type: vec![message("FooBar"), message("Foo_Bar")],
            ..Default::default()
        }];

        let response = generate_plugin_response(&request);

        let error = response
            .error
            .expect("nested Rust path collision should fail generation");
        assert!(error.contains(".hello.v1.Outer.FooBar"));
        assert!(error.contains(".hello.v1.Outer.Foo_Bar"));
        assert!(error.contains("both map to Rust path \"outer::FooBar\""));
        assert!(response.file.is_empty());
    }

    #[test]
    fn plugin_rejects_synthetic_map_entry_rpc_types() {
        let mut request = hello_request(None);
        request.proto_file[0].message_type.push(DescriptorProto {
            name: Some("Container".to_owned()),
            nested_type: vec![DescriptorProto {
                name: Some("LabelsEntry".to_owned()),
                options: Some(MessageOptions {
                    map_entry: Some(true),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        });
        request.proto_file[0].service[0].method[0].input_type =
            Some(".hello.v1.Container.LabelsEntry".to_owned());

        let response = generate_plugin_response(&request);

        let error = response
            .error
            .expect("synthetic map-entry RPC type should fail generation");
        assert!(error.contains("input type \".hello.v1.Container.LabelsEntry\""));
        assert!(error.contains("unresolved protobuf message type"));
        assert!(response.file.is_empty());
    }

    #[test]
    fn plugin_round_trips_codegen_protocol() {
        let mut input = Vec::new();
        hello_request(None)
            .encode(&mut input)
            .expect("request should encode");
        let mut output = Vec::new();

        super::run_plugin(input.as_slice(), &mut output).expect("plugin should run");

        let response = prost_types::compiler::CodeGeneratorResponse::decode(output.as_slice())
            .expect("response should decode");

        assert_eq!(response.error, None);
        assert_eq!(response.file.len(), 1);
    }

    fn hello_request(parameter: Option<String>) -> CodeGeneratorRequest {
        CodeGeneratorRequest {
            file_to_generate: vec!["proto/hello.proto".to_owned()],
            parameter,
            proto_file: vec![FileDescriptorProto {
                name: Some("proto/hello.proto".to_owned()),
                package: Some("hello.v1".to_owned()),
                message_type: vec![message("HelloRequest"), message("HelloReply")],
                service: vec![prost_types::ServiceDescriptorProto {
                    name: Some("Greeter".to_owned()),
                    method: vec![MethodDescriptorProto {
                        name: Some("SayHello".to_owned()),
                        input_type: Some(".hello.v1.HelloRequest".to_owned()),
                        output_type: Some(".hello.v1.HelloReply".to_owned()),
                        options: None,
                        client_streaming: Some(false),
                        server_streaming: Some(false),
                    }],
                    options: None,
                }],
                ..Default::default()
            }],
            compiler_version: None,
        }
    }

    fn message(name: &str) -> DescriptorProto {
        DescriptorProto {
            name: Some(name.to_owned()),
            ..Default::default()
        }
    }
}
