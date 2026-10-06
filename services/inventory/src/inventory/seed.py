from inventory.model.item import InventoryItem

SEED_DATA = {
    "KB-001": InventoryItem(sku="KB-001", name="Dell", quantity=3),
    "KB-002": InventoryItem(sku="KB-002", name="HP", quantity=2),
    "KB-003": InventoryItem(sku="KB-003", name="Microsoft", quantity=5),
    "KB-004": InventoryItem(sku="KB-004", name="Logitech", quantity=10),
}
