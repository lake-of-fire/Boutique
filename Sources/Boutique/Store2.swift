import Foundation
import Observation
import OrderedCollections

/// A MainActor wrapper that is Observable and suitable for SwiftUI, while using a background actor for concurrency.
/// - Immediately updates the UI on writes for responsiveness.
/// - Asynchronously synchronizes with the CoreStore.
/// - Merges background updates into its main-thread copy of items.
@MainActor
public final class Store2<Item: Codable & Sendable & Equatable> {
  public private(set) var items: [Item] = [] {
    didSet {
      notifyItemsChanged()
    }
  }

  public let core: CoreStore<Item>
  
  private let cacheIdentifier: KeyPath<Item, String>
  private var eventTask: Task<Void, Never>?

  // MARK: - Non-isolated Async API
  
  /// Asynchronously read all items from the core store.
  /// This is non-isolated and can be called from any context.
  nonisolated public var asyncItems: [Item] {
    get async {
      await core.items
    }
  }
  
  /// Start a batch of operations that will be emitted as a single event.
  /// Must be paired with a call to `asyncEndBatch()`.
  /// This allows multiple operations to be combined into a single update event.
  nonisolated public func asyncBeginBatch() async {
    await core.beginBatch()
  }
  
  /// End the current batch and emit all operations as a single event.
  nonisolated public func asyncEndBatch() async {
    await core.endBatch()
  }
  
  /// Execute multiple operations in a batch, emitting only a single event.
  /// - Parameter operations: An async closure that performs multiple core store operations
  nonisolated public func asyncBatch(_ operations: () async throws -> Void) async throws {
    await asyncBeginBatch()
    try await operations()
    await asyncEndBatch()
  }
  
  /// Asynchronously insert an item into the core store.
  /// This is non-isolated and can be called from any context.
  nonisolated public func asyncInsert(_ item: Item, firstRemovingExistingItems strategy: StoreItemRemovalStrategy<Item>? = nil) async throws {
    try await core.insert(item, firstRemovingExistingItems: strategy)
  }
  
  /// Asynchronously insert multiple items into the core store.
  /// This is non-isolated and can be called from any context.
  nonisolated public func asyncInsert(_ items: [Item], firstRemovingExistingItems strategy: StoreItemRemovalStrategy<Item>? = nil) async throws {
    try await core.insert(items, firstRemovingExistingItems: strategy)
  }
  
  /// Asynchronously remove an item from the core store.
  /// This is non-isolated and can be called from any context.
  nonisolated public func asyncRemove(_ item: Item) async throws {
    try await core.remove(item)
  }
  
  /// Asynchronously remove multiple items from the core store.
  /// This is non-isolated and can be called from any context.
  nonisolated public func asyncRemove(_ items: [Item]) async throws {
    try await core.remove(items)
  }
  
  /// Asynchronously remove all items from the core store.
  /// This is non-isolated and can be called from any context.
  nonisolated public func asyncRemoveAll() async throws {
    try await core.removeAll()
  }

  nonisolated public func asyncEvents() async -> AsyncStream<[StoreEvent<Item>]> {
    await core.events()
  }

  public func itemsStream() -> AsyncStream<[Item]> {
    AsyncStream { continuation in
      let box = ContinuationBox(continuation)
      itemsContinuations.append(box)

      continuation.yield(items)

      continuation.onTermination = { [weak self] _ in
        guard let self = self else { return }
        Task { @MainActor in
          self.itemsContinuations.removeAll { $0 === box }
        }
      }
    }
  }

  private var itemsContinuations: [ContinuationBox<Item>] = []

  public init(storage: StorageEngine, cacheIdentifier: KeyPath<Item, String>) async throws {
    self.cacheIdentifier = cacheIdentifier
    self.core = try await CoreStore(storage: storage, cacheIdentifier: cacheIdentifier)
    self.items = await core.items

    // Subscribe to core store events
    self.eventTask = Task {
      let stream = await core.events()
      for await events in stream {
        applyEvents(events)
      }
    }
  }

  // MARK: - Main-Actor Batching

  /// Represents the state of a batch operation
  private struct BatchState {
    var items: [Item]
    var operations: [() async throws -> Void]
    var previousItems: [Item] // For rollback
  }
  
  private var currentBatch: BatchState?
  private var batchDepth = 0

  /// Start a batch of operations.
  /// - Batches can be nested, with changes only being applied when the outermost batch completes
  /// - All operations within the batch are treated as a single transaction
  /// - The UI will only update once when the entire batch succeeds
  public func beginBatch() {
    batchDepth += 1
    // Only create new batch state for the outermost batch
    if batchDepth == 1 {
      currentBatch = BatchState(
        items: items,
        operations: [],
        previousItems: items
      )
    }
  }

  /// End the current batch and apply all changes if this is the outermost batch.
  /// - If any operation fails, all changes are reverted
  public func endBatch() {
    guard batchDepth > 0 else { return }
    batchDepth -= 1
    
    // Only process the batch when we're ending the outermost batch
    guard batchDepth == 0, let batch = currentBatch else { return }
    
    // Create task for async work
    Task(priority: .userInitiated) { [weak self, batch] in
      guard let self = self else { return }
      
      do {
        // Start core store batch
        await core.beginBatch()
        
        // Execute all operations
        for operation in batch.operations {
          try await operation()
        }
        
        // Complete core store batch
        await core.endBatch()
        
      } catch {
        // On any error, revert to previous state
        await MainActor.run { [batch] in
          self.items = batch.previousItems
        }
        
        // End core store batch
        await core.endBatch()
      }
    }
    
    // Clear batch state
    currentBatch = nil
  }

  /// Execute operations in a batch synchronously.
  /// - Parameter operations: A closure containing the operations to batch
  public func batch(_ operations: () -> Void) {
    beginBatch()
    operations()
    endBatch()
  }

  // MARK: - Main-Actor Writes (Optimistic UI Updates)

  private func updateState(
    _ update: (inout [Item]) -> Void,
    operation: @escaping () async throws -> Void,
    rollback: @escaping ([Item]) -> [Item]
  ) {
    if var batch = currentBatch {
      // Update batch state
      update(&batch.items)
      batch.operations.append(operation)
      currentBatch = batch
    } else {
      // Immediate update
      update(&items)
      
      Task(priority: .userInitiated) {
        do {
          try await operation()
        } catch {
          // Use specialized rollback function to revert only the failed changes
          await MainActor.run {
            self.items = rollback(self.items)
          }
        }
      }
    }
  }

  public func insert(_ item: Item, firstRemovingExistingItems strategy: StoreItemRemovalStrategy<Item>? = nil) {
    updateState({ items in
      if let strategy = strategy {
        let itemsToRemove = strategy.removedItems([item])
        items = removeLocal(items: items, itemsToRemove: itemsToRemove)
      }
      items = upsertLocal(items: items, newItem: item)
    }, operation: { [core, item, strategy] in
      try await core.insert(item, firstRemovingExistingItems: strategy)
    }, rollback: { [weak self] items in
      guard let self = self else { return items }
      return self.revertLocalChanges(forInsertedItems: [item], items: items)
    })
  }

  public func insert(_ itemsToInsert: [Item], firstRemovingExistingItems strategy: StoreItemRemovalStrategy<Item>? = nil) {
    updateState({ items in
      if let strategy = strategy {
        let itemsToRemove = strategy.removedItems(itemsToInsert)
        items = removeLocal(items: items, itemsToRemove: itemsToRemove)
      }
      items = upsertLocal(items: items, newItems: itemsToInsert)
    }, operation: { [core, itemsToInsert, strategy] in
      try await core.insert(itemsToInsert, firstRemovingExistingItems: strategy)
    }, rollback: { [weak self] items in
      guard let self = self else { return items }
      return self.revertLocalChanges(forInsertedItems: itemsToInsert, items: items)
    })
  }

  public func remove(_ item: Item) {
    updateState({ items in
      items = removeLocal(items: items, itemsToRemove: [item])
    }, operation: { [core, item] in
      try await core.remove(item)
    }, rollback: { [weak self] items in
      guard let self = self else { return items }
      return self.revertLocalChanges(forRemovedItems: [item], currentItems: items)
    })
  }

  public func remove(_ itemsToRemove: [Item]) {
    updateState({ items in
      items = removeLocal(items: items, itemsToRemove: itemsToRemove)
    }, operation: { [core, itemsToRemove] in
      try await core.remove(itemsToRemove)
    }, rollback: { [weak self] items in
      guard let self = self else { return items }
      return self.revertLocalChanges(forRemovedItems: itemsToRemove, currentItems: items)
    })
  }

  public func removeAll() {
    let previousItems = items
    updateState({ items in
      items.removeAll()
    }, operation: { [core] in
      try await core.removeAll()
    }, rollback: { [weak self] items in
      guard let self = self else { return items }
      return self.revertLocalChanges(forRemovedItems: previousItems, currentItems: items)
    })
  }

  // MARK: - Private Helpers

  private func applyEvents(_ events: [StoreEvent<Item>]) {
    let previousItems = items
    var newItems = previousItems
    
    for event in events {
      switch event {
      case .initial:
        // If needed, handle an initial state event
        break
      case .loaded(let loadedItems):
        newItems = loadedItems
      case .insert(let insertedItems):
        newItems = upsertLocal(items: newItems, newItems: insertedItems)
      case .remove(let removedItems):
        newItems = removeLocal(items: newItems, itemsToRemove: removedItems)
      }
    }
    
    if newItems != previousItems {
      items = newItems
    }
  }

  /// Insert or update items in the local array.
  private func upsertLocal(items: [Item], newItems: [Item]) -> [Item] {
    // Create dictionary preserving order of existing items
    var dictionary = OrderedDictionary(
      uniqueKeys: items.map { $0[keyPath: cacheIdentifier] },
      values: items
    )
    // Append new items in order they were provided, just like CoreStore
    for item in newItems {
      // If key exists, this updates in place preserving position
      // If key is new, appends to end like CoreStore
      dictionary[item[keyPath: cacheIdentifier]] = item
    }
    return Array(dictionary.values)
  }

  private func upsertLocal(items: [Item], newItem: Item) -> [Item] {
    upsertLocal(items: items, newItems: [newItem])
  }

  /// Remove items from the local array.
  private func removeLocal(items: [Item], itemsToRemove: [Item]) -> [Item] {
    // Create dictionary preserving order of existing items
    var dictionary = OrderedDictionary(
      uniqueKeys: items.map { $0[keyPath: cacheIdentifier] },
      values: items
    )
    // Remove items while maintaining relative positions of remaining items
    for item in itemsToRemove {
      dictionary.removeValue(forKey: item[keyPath: cacheIdentifier])
    }
    return Array(dictionary.values)
  }

  /// If insertion fails, try to revert the inserted items.
  /// In a more advanced scenario, you might want to fetch the authoritative state from `core`.
  private func revertLocalChanges(forInsertedItems insertedItems: [Item], items: [Item]) -> [Item] {
    // Create dictionary preserving order of existing items
    var dictionary = OrderedDictionary(
      uniqueKeys: items.map { $0[keyPath: cacheIdentifier] },
      values: items
    )
    // Remove items while maintaining relative positions
    for item in insertedItems {
      dictionary.removeValue(forKey: item[keyPath: cacheIdentifier])
    }
    return Array(dictionary.values)
  }

  /// If removal fails, reinsert the removed items into the current items array
  /// This preserves any other changes that might have happened in the meantime
  private func revertLocalChanges(forRemovedItems removedItems: [Item], currentItems: [Item]) -> [Item] {
    // Use the existing upsert logic to reinsert the items
    return upsertLocal(items: currentItems, newItems: removedItems)
  }

  private func notifyItemsChanged() {
    for box in itemsContinuations {
      box.continuation.yield(items)
    }
  }
}

private final class ContinuationBox<Item: Equatable> {
  let continuation: AsyncStream<[Item]>.Continuation

  init(_ continuation: AsyncStream<[Item]>.Continuation) {
    self.continuation = continuation
  }
}
