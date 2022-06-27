use pegasus::{BuildJobError, Worker};

pub trait JobAssembly: Send + Sync + 'static {
    fn assemble(
        &self, job: &mut Vec<u8>, worker: &mut Worker<Vec<u8>, Vec<u8>>,
    ) -> Result<(), BuildJobError>;
}

// pub struct DynLibraryAssembly;
//
// impl JobAssembly for DynLibraryAssembly {
//     fn assemble(&self, job: &JobDesc, worker: &mut Worker<Vec<u8>, Vec<u8>>) -> Result<(), BuildJobError> {
//         if let Ok(resource) = String::from_utf8(job.resource.clone()) {
//             if let Some(lib) = pegasus::resource::get_global_resource::<Library>(&resource) {
//                 info!("load library {};", resource);
//                 let func: Symbol<unsafe extern "Rust" fn(&[u8], &mut Worker<Vec<u8>, Vec<u8>>) -> Result<(), BuildJobError>> = unsafe {
//                     match lib.get(&job.plan[..]) {
//                         Ok(sym) => sym,
//                         Err(e) => {
//                             return Err(format!("fail to link, because {:?}", e))?;
//                         }
//                     }
//                 };
//                 unsafe { func(&job.input, worker) }
//             } else {
//                 Err(format!("libarry with name {} not found;", resource))?
//             }
//         } else {
//             Err("illegal library name;")?
//         }
//     }
// }
