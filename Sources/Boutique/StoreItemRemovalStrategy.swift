import Foundation

public struct StoreItemRemovalStrategy<RemovedItem: Codable> {
  var removedItems: ([RemovedItem]) -> [RemovedItem]

  /// Removes all of the items from the in-memory and the StorageEngine cache before saving new items.
  static var all: StoreItemRemovalStrategy {
    StoreItemRemovalStrategy(removedItems: { $0 })
  }

  /// Removes specific items from the Store and disk cache before saving new items.
  static func items(_ itemsToRemove: [RemovedItem]) -> StoreItemRemovalStrategy {
    StoreItemRemovalStrategy(removedItems: { _ in itemsToRemove })
  }
}
