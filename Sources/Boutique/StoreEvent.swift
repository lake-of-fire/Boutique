import Foundation

/// Represents changes to the store.
public enum StoreEvent<Item: Codable & Sendable> {
  case initial
  case loaded([Item])
  case insert([Item])
  case remove([Item])
}
