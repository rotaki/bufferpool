use rand::distributions::uniform::SampleUniform;
use rand::distributions::Alphanumeric;
use rand::{
    distributions::{Distribution, Uniform},
    thread_rng, Rng,
};

/// Generates a random alphanumeric string of a specified length.
///
/// # Arguments
///
/// * `length` - The length of the string to generate.
pub fn gen_random_string_with_length(length: usize) -> String {
    thread_rng()
        .sample_iter(Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

pub fn gen_random_byte_vec_with_length(length: usize) -> Vec<u8> {
    let mut rng = thread_rng();
    let range = Uniform::new_inclusive(0, 255);
    let mut vec = Vec::with_capacity(length);
    for _ in 0..length {
        vec.push(range.sample(&mut rng) as u8);
    }
    vec
}

/// Generates a random integer within a specified range.
///
/// # Arguments
///
/// * `min` - The minimum value of the integer (inclusive).
/// * `max` - The maximum value of the integer (inclusive).
pub fn gen_random_int<T>(min: T, max: T) -> T
where
    T: SampleUniform,
{
    let mut rng = thread_rng();
    rng.sample(Uniform::new_inclusive(min, max))
}

pub fn gen_random_string(min: usize, max: usize) -> String {
    let length = gen_random_int(min, max);
    gen_random_string_with_length(length)
}

pub fn gen_random_byte_vec(min: usize, max: usize) -> Vec<u8> {
    let length = gen_random_int(min, max);
    gen_random_byte_vec_with_length(length)
}
