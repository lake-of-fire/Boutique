@_exported import Bodega
import Foundation
import OrderedCollections

public actor CoreStore<Item: Codable & Sendable> {
    private let storageEngine: StorageEngine
    private let cacheIdentifier: KeyPath<Item, String>
    private var itemsDictionary: OrderedDictionary<String, Item> = [:]
    private var isBatching = false
    private var batchedEvents: [StoreEvent<Item>] = []
    
    // A subject-like mechanism for broadcasting events:
    // Each new AsyncStream created from `events()` will pick up any new events.
    // Note: AsyncStream provides a mechanism to handle multiple subscribers easily.
    private var eventContinuations = [AsyncStream<[StoreEvent<Item>]>.Continuation]()
    
    public init(storage: StorageEngine, cacheIdentifier: KeyPath<Item, String>) async throws {
        self.storageEngine = storage
        self.cacheIdentifier = cacheIdentifier
        
        let storedData = await storageEngine.readAllData()
        let loadedItems = try storedData.map { try JSONCoders.decoder.decode(Item.self, from: $0) }
        
        // Build dictionary from loaded items
        self.itemsDictionary = OrderedDictionary(
            uniqueKeys: loadedItems.map { $0[keyPath: cacheIdentifier] },
            values: loadedItems
        )
    }
    
    /// Start a batch of operations that will be emitted as a single event
    public func beginBatch() {
        isBatching = true
        batchedEvents = []
    }
    
    /// End the current batch and emit all operations as a single event
    public func endBatch() {
        guard isBatching else { return }
        isBatching = false
        
        if !batchedEvents.isEmpty {
            emit(batchedEvents)
        }
        batchedEvents = []
    }
    
    public func batch(_ operations: () async throws -> Void) async throws {
        beginBatch()
        try await operations()
        endBatch()
    }
    
    public func events() -> AsyncStream<[StoreEvent<Item>]> {
        AsyncStream { continuation in
            eventContinuations.append(continuation)
            // Emit initial loaded event:
            continuation.yield([.loaded(Array(itemsDictionary.values))])
        }
    }
    
    public var items: [Item] {
        get async {
            Array(itemsDictionary.values)
        }
    }
    
    // Core insert functions
    public func insert(_ items: [Item], firstRemovingExistingItems strategy: StoreItemRemovalStrategy<Item>? = nil) async throws {
        if let strategy = strategy {
            let itemsToRemove = strategy.removedItems(items)
            if !itemsToRemove.isEmpty {
                try await remove(itemsToRemove)
            }
        }
        
        for item in items {
            itemsDictionary[item[keyPath: cacheIdentifier]] = item
        }
        
        let dataAndKeys = try items.map { item in
            (CacheKey(item[keyPath: cacheIdentifier]), try JSONCoders.encoder.encode(item))
        }
        try await storageEngine.write(dataAndKeys)
        
        if isBatching {
            batchedEvents.append(.insert(items))
        } else {
            emit(.insert(items))
        }
    }
    
    public func insert(_ item: Item, firstRemovingExistingItems strategy: StoreItemRemovalStrategy<Item>? = nil) async throws {
        try await insert([item], firstRemovingExistingItems: strategy)
    }
    
    // Core remove functions
    public func remove(_ items: [Item]) async throws {
        let keys = items.map { $0[keyPath: cacheIdentifier] }
        let cacheKeys = keys.map { CacheKey($0) }
        try await storageEngine.remove(keys: cacheKeys)
        for key in keys {
            itemsDictionary.removeValue(forKey: key)
        }
        
        if isBatching {
            batchedEvents.append(.remove(items))
        } else {
            emit(.remove(items))
        }
    }
    
    public func remove(_ item: Item) async throws {
        try await remove([item])
    }
    
    public func removeByID(_ id: String) async throws {
        guard let item = itemsDictionary[id] else { return }
        try await remove([item])
    }
    
    public func removeAll() async throws {
        try await storageEngine.removeAllData()
        let oldItems = Array(itemsDictionary.values)
        itemsDictionary.removeAll()
        
        if isBatching {
            batchedEvents.append(.remove(oldItems))
        } else {
            emit(.remove(oldItems))
        }
    }
    
    private func emit(_ event: StoreEvent<Item>) {
        emit([event])
    }
    
    private func emit(_ events: [StoreEvent<Item>]) {
        for continuation in eventContinuations {
            continuation.yield(events)
        }
    }
}
