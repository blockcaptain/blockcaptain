use std::path::Path;
use pnsystem::filesystem;

fn main() {
    let foo = filesystem::get_device_info("/dev/vda");
    let bar = filesystem::get_device_ids("/dev/vda");
    let baz = filesystem::get_mountpoint(Path::new("/"));
    println!("{:#?} -- {:#?} -- {:#?}", foo, bar, baz);
}
