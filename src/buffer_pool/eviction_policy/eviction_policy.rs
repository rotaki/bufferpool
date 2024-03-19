/*
 * eviction_policy.rs
 *
 * This file defines the trait for eviction policy, which can be implemented by
 * different eviction policies.
 */

pub trait EvictionPolicy {
    fn choose_victim(&mut self) -> usize;
    fn reset(&mut self, index: usize);
    fn reset_all(&mut self);
    fn update(&mut self, index: usize);
}
