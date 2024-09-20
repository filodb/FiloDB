//! Extensions to JNIEnv

use jni::{
    objects::{JByteArray, JObject, JObjectArray, JString},
    JNIEnv,
};

use crate::errors::JavaResult;

/// Helper extensions for working with JVM types
#[allow(dead_code)]
pub trait JNIEnvExt<'a> {
    /// Get a rust string from Java String
    fn get_rust_string(&mut self, obj: &JString) -> JavaResult<String>;

    /// Get a class name and return it as a string
    /// This is equivilant to Java code `obj.class.name`
    fn get_object_class_name(&mut self, obj: &JObject) -> JavaResult<String>;

    /// Run a closure over every String in a String[]
    fn foreach_string_in_array<F>(&mut self, array: &JObjectArray, func: F) -> JavaResult<()>
    where
        F: FnMut(String) -> JavaResult<()>;

    /// Get a byte array from the JVM
    fn get_byte_array_offset_len(
        &mut self,
        array: &JByteArray,
        offset: usize,
        len: usize,
    ) -> JavaResult<Vec<u8>>;

    /// Get a byte array from the JVM
    fn get_byte_array(&mut self, array: &JByteArray) -> JavaResult<Vec<u8>>;
}

impl<'a> JNIEnvExt<'a> for JNIEnv<'a> {
    fn get_rust_string(&mut self, obj: &JString) -> JavaResult<String> {
        let ret = self.get_string(obj)?.into();
        Ok(ret)
    }

    fn get_object_class_name(&mut self, obj: &JObject) -> JavaResult<String> {
        let class = self.get_object_class(obj)?;
        let name = self
            .get_field(&class, "name", "Ljava/lang/String;")?
            .l()?
            .into();

        let ret = self.get_string(&name)?.into();
        Ok(ret)
    }

    fn foreach_string_in_array<F>(&mut self, array: &JObjectArray, mut func: F) -> JavaResult<()>
    where
        F: FnMut(String) -> JavaResult<()>,
    {
        let len = self.get_array_length(array)?;
        for idx in 0..len {
            let s = self.get_object_array_element(array, idx)?.into();
            let s = self.get_rust_string(&s)?;
            func(s)?;
        }

        Ok(())
    }

    fn get_byte_array_offset_len(
        &mut self,
        array: &JByteArray,
        offset: usize,
        len: usize,
    ) -> JavaResult<Vec<u8>> {
        let mut bytes = vec![0u8; len];
        let bytes_ptr = bytes.as_mut_ptr() as *mut i8;
        let bytes_ptr = unsafe { std::slice::from_raw_parts_mut(bytes_ptr, len) };

        self.get_byte_array_region(array, offset as i32, bytes_ptr)?;

        Ok(bytes)
    }

    fn get_byte_array(&mut self, array: &JByteArray) -> JavaResult<Vec<u8>> {
        let len = self.get_array_length(array)?;

        self.get_byte_array_offset_len(array, 0, len as usize)
    }
}
