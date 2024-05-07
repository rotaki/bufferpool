use std::collections::VecDeque;

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

pub fn gen_random_permutation<T>(mut vec: Vec<T>) -> Vec<T> {
    let len = vec.len();
    for i in 0..len {
        let j = gen_random_int(i, len - 1);
        vec.swap(i, j);
    }
    vec
}

use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize, Clone)]
pub struct RandomKVs {
    kvs: Vec<(usize, Vec<u8>)>,
}

impl RandomKVs {
    pub fn new(num_keys: usize, val_min_size: usize, val_max_size: usize) -> Self {
        let keys = (0..num_keys).collect::<Vec<usize>>();
        let keys = gen_random_permutation(keys);
        let vals = (0..num_keys)
            .map(|_| gen_random_byte_vec(val_min_size, val_max_size))
            .collect::<Vec<Vec<u8>>>();
        let kvs = keys
            .iter()
            .zip(vals.iter())
            .map(|(&k, v)| (k, v.clone()))
            .collect();
        RandomKVs { kvs }
    }

    pub fn len(&self) -> usize {
        self.kvs.len()
    }

    pub fn partition(&self, num_partitions: usize) -> VecDeque<Self> {
        let mut partitions = VecDeque::new();
        let partition_size = self.len() / num_partitions;
        let mut start = 0;
        for i in 0..num_partitions {
            let end = if i == num_partitions - 1 {
                self.len()
            } else {
                start + partition_size
            };
            let partition = RandomKVs {
                kvs: self.kvs[start..end].to_vec(),
            };
            partitions.push_back(partition);
            start = end;
        }
        partitions
    }

    pub fn iter(&self) -> impl Iterator<Item = (&usize, &Vec<u8>)> {
        self.kvs.iter().map(|(k, v)| (k, v))
    }

    pub fn get(&self, key: &usize) -> Option<&Vec<u8>> {
        self.kvs.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }
}

impl std::ops::Index<usize> for RandomKVs {
    type Output = (usize, Vec<u8>);

    fn index(&self, index: usize) -> &Self::Output {
        &self.kvs[index]
    }
}
