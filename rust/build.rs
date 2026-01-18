use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let proto_root = "../protos";
    let files = [
        "../protos/feast/serving/ServingService.proto",
        "../protos/feast/serving/TransformationService.proto",
        "../protos/feast/types/Value.proto",
        "../protos/feast/types/EntityKey.proto",
        "../protos/feast/core/Registry.proto",
    ];

    for file in &files {
        println!("cargo:rerun-if-changed={}", file);
    }

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .include_file("feast.rs")
        .compile_protos(&files, &[proto_root])?;

    Ok(())
}
