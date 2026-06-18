#![forbid(unsafe_code)]
#![allow(
    clippy::format_push_string,
    clippy::missing_errors_doc,
    clippy::module_name_repetitions
)]

use prost_build::{Method, Service, ServiceGenerator};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrevRpcServiceGenerator {
    runtime_path: String,
}

impl Default for TrevRpcServiceGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl TrevRpcServiceGenerator {
    #[must_use]
    pub fn new() -> Self {
        Self {
            runtime_path: "::trevrpc".to_owned(),
        }
    }

    #[must_use]
    pub fn runtime_path(mut self, runtime_path: impl Into<String>) -> Self {
        self.runtime_path = runtime_path.into();
        self
    }
}

#[must_use]
pub fn service_generator() -> TrevRpcServiceGenerator {
    TrevRpcServiceGenerator::new()
}

pub fn configure(config: &mut prost_build::Config) -> &mut prost_build::Config {
    config.service_generator(Box::new(service_generator()))
}

impl ServiceGenerator for TrevRpcServiceGenerator {
    fn generate(&mut self, service: Service, buf: &mut String) {
        generate_service(&self.runtime_path, &service, buf);
    }
}

fn generate_service(runtime_path: &str, service: &Service, buf: &mut String) {
    let unsupported_methods = service
        .methods
        .iter()
        .filter(|method| method.client_streaming || method.server_streaming);

    for method in unsupported_methods {
        let message = format!(
            "TrevRPC only supports unary RPCs right now: {}.{}",
            service.proto_name, method.proto_name
        );
        buf.push_str(&format!("compile_error!({message:?});\n"));
    }

    if service
        .methods
        .iter()
        .any(|method| method.client_streaming || method.server_streaming)
    {
        return;
    }

    service.comments.append_with_indent(0, buf);
    generate_trait(runtime_path, service, buf);
    generate_client(runtime_path, service, buf);
    generate_registration(runtime_path, service, buf);
}

fn generate_trait(runtime_path: &str, service: &Service, buf: &mut String) {
    buf.push_str(&format!(
        "#[allow(clippy::missing_errors_doc)]\n#[{runtime_path}::async_trait]\npub trait {}: Send + Sync + 'static {{\n",
        service.name
    ));

    for method in &service.methods {
        method.comments.append_with_indent(1, buf);
        buf.push_str(&format!(
            "    async fn {}(&self, request: {}) -> ::core::result::Result<{}, {runtime_path}::Status>;\n",
            method.name, method.input_type, method.output_type
        ));
    }

    buf.push_str("}\n\n");
}

fn generate_client(runtime_path: &str, service: &Service, buf: &mut String) {
    let client_name = format!("{}Client", service.name);
    let service_name = service_path(service);

    buf.push_str(&format!(
        "#[derive(Clone)]\npub struct {client_name}<T> {{\n    transport: T,\n}}\n\n"
    ));

    buf.push_str(&format!(
        "impl<T> {client_name}<T> {{\n    pub const SERVICE: &'static str = {service_name:?};\n\n    #[must_use]\n    pub fn new(transport: T) -> Self {{\n        Self {{ transport }}\n    }}\n\n    #[must_use]\n    pub const fn transport(&self) -> &T {{\n        &self.transport\n    }}\n\n    #[must_use]\n    pub fn into_transport(self) -> T {{\n        self.transport\n    }}\n}}\n\n"
    ));

    buf.push_str(&format!(
        "#[allow(clippy::missing_errors_doc)]\nimpl<T> {client_name}<T>\nwhere\n    T: {runtime_path}::client::RpcTransport,\n{{\n"
    ));

    for method in &service.methods {
        generate_client_method(runtime_path, method, buf);
    }

    buf.push_str("}\n\n");
}

fn generate_client_method(runtime_path: &str, method: &Method, buf: &mut String) {
    buf.push_str(&format!(
        "    pub async fn {}(&self, request: {}) -> ::core::result::Result<{}, {runtime_path}::Error> {{\n        {runtime_path}::client::unary(&self.transport, Self::SERVICE, {:?}, &request).await\n    }}\n\n",
        method.name, method.input_type, method.output_type, method.proto_name
    ));
}

fn generate_registration(runtime_path: &str, service: &Service, buf: &mut String) {
    let register_name = format!("register_{}", to_snake_case(&service.name));
    let service_name = service_path(service);

    buf.push_str(&format!(
        "pub fn {register_name}<S>(server: &mut {runtime_path}::server::Server, service: S)\nwhere\n    S: {},\n{{\n    let service = ::std::sync::Arc::new(service);\n",
        service.name
    ));

    for method in &service.methods {
        generate_registration_method(&service_name, method, buf);
    }

    buf.push_str("}\n");
}

fn generate_registration_method(service_name: &str, method: &Method, buf: &mut String) {
    buf.push_str(&format!(
        "\n    {{\n        let service = ::std::sync::Arc::clone(&service);\n        server.route({service_name:?}, {:?}, move |body| {{\n            let service = ::std::sync::Arc::clone(&service);\n            async move {{\n                let request = <{} as ::prost::Message>::decode(body.as_slice())?;\n                let response = service.{}(request).await?;\n                Ok(::prost::Message::encode_to_vec(&response))\n            }}\n        }});\n    }}\n",
        method.proto_name, method.input_type, method.name
    ));
}

fn service_path(service: &Service) -> String {
    if service.package.is_empty() {
        service.proto_name.clone()
    } else {
        format!("{}.{}", service.package, service.proto_name)
    }
}

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
    use prost_build::{Comments, Method, Service};

    use super::{generate_service, service_path, to_snake_case};

    #[test]
    fn builds_proto_service_paths() {
        let service = Service {
            name: "Greeter".to_owned(),
            proto_name: "Greeter".to_owned(),
            package: "hello.v1".to_owned(),
            comments: Comments::default(),
            methods: Vec::new(),
            options: Default::default(),
        };

        assert_eq!(service_path(&service), "hello.v1.Greeter");
    }

    #[test]
    fn converts_service_names_to_register_function_names() {
        assert_eq!(to_snake_case("Greeter"), "greeter");
        assert_eq!(to_snake_case("ChatService"), "chat_service");
    }

    #[test]
    fn generates_unary_service_api() {
        let service = Service {
            name: "Greeter".to_owned(),
            proto_name: "Greeter".to_owned(),
            package: "hello.v1".to_owned(),
            comments: Comments::default(),
            methods: vec![Method {
                name: "say_hello".to_owned(),
                proto_name: "SayHello".to_owned(),
                comments: Comments::default(),
                input_type: "HelloRequest".to_owned(),
                output_type: "HelloReply".to_owned(),
                input_proto_type: ".hello.v1.HelloRequest".to_owned(),
                output_proto_type: ".hello.v1.HelloReply".to_owned(),
                options: Default::default(),
                client_streaming: false,
                server_streaming: false,
            }],
            options: Default::default(),
        };
        let mut generated = String::new();

        generate_service("::trevrpc", &service, &mut generated);

        assert!(generated.contains("pub trait Greeter"));
        assert!(generated.contains("pub struct GreeterClient<T>"));
        assert!(generated.contains("pub fn register_greeter<S>"));
        assert!(generated.contains("hello.v1.Greeter"));
        assert!(generated.contains("SayHello"));
    }
}
