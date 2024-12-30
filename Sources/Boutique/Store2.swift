import Foundation
import Observation
import OrderedCollections

/// A MainActor wrapper that is Observable and suitable for SwiftUI, while using a background actor for concurrency.
/// - Immediately updates the UI on writes for responsiveness.
/// - Asynchronously synchronizes with the CoreStore.
/// - Merges background updates into its main-thread copy of items.
@MainActor
@Observable
public final class Store2<Item: Codable & Sendable & Equatable> {
  public private(set) var items: [Item] = []

  private let coreStore: CoreStore<Item>
  private let cacheIdentifier: KeyPath<Item, String>
  private var eventTask: Task<Void, Never>?

  public init(storage: StorageEngine, cacheIdentifier: KeyPath<Item, String>) async throws {
    self.cacheIdentifier = cacheIdentifier
    self.coreStore = try await CoreStore(storage: storage, cacheIdentifier: cacheIdentifier)
    self.items = await coreStore.allItems()

    // Subscribe to core store events
    self.eventTask = Task {
      let stream = await coreStore.events()
      for await event in stream {
        applyEvent(event)
      }
    }
  }

  // MARK: - Main-Actor Writes (Optimistic UI Updates)

  public func insert(_ item: Item, firstRemovingExistingItems strategy: StoreItemRemovalStrategy<Item>? = nil) {
    // Optimistically update UI
    if let strategy = strategy {
      let itemsToRemove = strategy.removedItems([item])
      items = removeLocal(items: items, itemsToRemove: itemsToRemove)
    }
    items = upsertLocal(items: items, newItem: item)

    // Forward write to actor
    Task {
      do {
        try await coreStore.insert(item, firstRemovingExistingItems: strategy)
      } catch {
        // Revert changes if failed
        items = revertLocalChanges(forInsertedItems: [item], items: items)
      }
    }
  }

  public func insert(_ itemsToInsert: [Item], firstRemovingExistingItems strategy: StoreItemRemovalStrategy<Item>? = nil) {
    // Optimistically update UI
    if let strategy = strategy {
      let itemsToRemove = strategy.removedItems(itemsToInsert)
      items = removeLocal(items: items, itemsToRemove: itemsToRemove)
    }
    items = upsertLocal(items: items, newItems: itemsToInsert)

    Task {
      do {
        try await coreStore.insert(itemsToInsert, firstRemovingExistingItems: strategy)
      } catch {
        // Revert changes if failed
        items = revertLocalChanges(forInsertedItems: itemsToInsert, items: items)
      }
    }
  }

  public func remove(_ item: Item) {
    // Optimistically update UI
    items = removeLocal(items: items, itemsToRemove: [item])

    Task {
      do {
        try await coreStore.remove(item)
      } catch {
        // Revert using intelligent revert method
        items = revertLocalChanges(forRemovedItems: [item], currentItems: items)
      }
    }
  }

  public func remove(_ itemsToRemove: [Item]) {
    items = removeLocal(items: items, itemsToRemove: itemsToRemove)

    Task {
      do {
        try await coreStore.remove(itemsToRemove)
      } catch {
        // Revert using intelligent revert method
        items = revertLocalChanges(forRemovedItems: itemsToRemove, currentItems: items)
      }
    }
  }

  public func removeAll() {
    let previousItems = items
    items.removeAll()

    Task {
      do {
        try await coreStore.removeAll()
      } catch {
        // Revert if fails
        items = revertLocalChanges(forRemovedItems: previousItems, currentItems: items)
      }
    }
  }

  // MARK: - Private Helpers

  private func applyEvent(_ event: StoreEvent<Item>) {
    let previousItems = items
    var newItems = previousItems
    switch event {
    case .initial:
      // If needed, handle an initial state event
      break
    case .loaded(let loadedItems):
      newItems = loadedItems
    case .insert(let insertedItems):
      newItems = upsertLocal(items: previousItems, newItems: insertedItems)
    case .remove(let removedItems):
      newItems = removeLocal(items: previousItems, itemsToRemove: removedItems)
    }
    if newItems != previousItems {
      items = newItems
    }
  }

  /// Insert or update items in the local array.
  private func upsertLocal(items: [Item], newItems: [Item]) -> [Item] {
    var dictionary = Dictionary(
      uniqueKeysWithValues: items.map { ($0[keyPath: cacheIdentifier], $0) })
    for item in newItems {
      dictionary[item[keyPath: cacheIdentifier]] = item
    }
    return Array(dictionary.values)
  }

  private func upsertLocal(items: [Item], newItem: Item) -> [Item] {
    upsertLocal(items: items, newItems: [newItem])
  }

  /// Remove items from the local array.
  private func removeLocal(items: [Item], itemsToRemove: [Item]) -> [Item] {
    let keysToRemove = Set(itemsToRemove.map { $0[keyPath: cacheIdentifier] })
    return items.filter { !keysToRemove.contains($0[keyPath: cacheIdentifier]) }
  }

  /// If insertion fails, try to revert the inserted items.
  /// In a more advanced scenario, you might want to fetch the authoritative state from `coreStore`.
  private func revertLocalChanges(forInsertedItems insertedItems: [Item], items: [Item]) -> [Item] {
    let keys = Set(insertedItems.map { $0[keyPath: cacheIdentifier] })
    return items.filter { !keys.contains($0[keyPath: cacheIdentifier]) }
  }

  /// If removal fails, reinsert the removed items into the current items array
  /// This preserves any other changes that might have happened in the meantime
  private func revertLocalChanges(forRemovedItems removedItems: [Item], currentItems: [Item]) -> [Item] {
    // Use the existing upsert logic to reinsert the items
    return upsertLocal(items: currentItems, newItems: removedItems)
  }
}
