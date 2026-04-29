// MongoDB Product Catalog Initialization

// Switch to product_catalog_db
db = db.getSiblingDB('product_catalog_db');

// Create collections
db.createCollection('products');
db.createCollection('categories');
db.createCollection('reviews');

// Create indexes
db.products.createIndex({ name: 1 });
db.products.createIndex({ category: 1 });
db.products.createIndex({ price: 1 });
db.products.createIndex({ createdAt: -1 });
db.products.createIndex({ stock_reference_id: 1 });
db.products.createIndex({'$**': 'text'}); // Full-text search

db.categories.createIndex({ name: 1 }, { unique: true });

db.reviews.createIndex({ product_id: 1 });
db.reviews.createIndex({ user_id: 1 });
db.reviews.createIndex({ createdAt: -1 });

// Insert seed products
db.products.insertMany([
    {
        _id: 1,
        name: "Wireless Headphones",
        description: "High-quality wireless headphones with noise cancellation",
        price: 129.99,
        category: "Electronics",
        stock_reference_id: 1,
        brand: "TechBrand",
        ratings: 4.5,
        reviews_count: 245,
        images: [
            "/images/headphones1.jpg",
            "/images/headphones2.jpg"
        ],
        specifications: {
            battery_life: "40 hours",
            connectivity: "Bluetooth 5.0",
            driver_size: "40mm",
            impedance: "32 Ohms"
        },
        is_active: true,
        createdAt: new Date(),
        updatedAt: new Date()
    },
    {
        _id: 2,
        name: "USB-C Cable",
        description: "Durable USB-C charging and data transfer cable",
        price: 44.99,
        category: "Accessories",
        stock_reference_id: 2,
        brand: "ChargeMax",
        ratings: 4.7,
        reviews_count: 512,
        images: [
            "/images/cable1.jpg"
        ],
        specifications: {
            length: "2 meters",
            material: "Braided Nylon",
            max_current: "5A",
            certification: "USB-C 3.1"
        },
        is_active: true,
        createdAt: new Date(),
        updatedAt: new Date()
    },
    {
        _id: 3,
        name: "Laptop Stand",
        description: "Premium adjustable laptop stand for better ergonomics",
        price: 79.99,
        category: "Office",
        stock_reference_id: 3,
        brand: "ErgoDesign",
        ratings: 4.6,
        reviews_count: 189,
        images: [
            "/images/stand1.jpg",
            "/images/stand2.jpg",
            "/images/stand3.jpg"
        ],
        specifications: {
            material: "Aluminum Alloy",
            max_weight: "15 kg",
            adjustability: "0-40 degrees",
            compatibility: "10-17 inch laptops"
        },
        is_active: true,
        createdAt: new Date(),
        updatedAt: new Date()
    },
    {
        _id: 4,
        name: "Mechanical Keyboard",
        description: "Professional mechanical keyboard with RGB lighting",
        price: 199.99,
        category: "Electronics",
        stock_reference_id: 4,
        brand: "KeyMaster",
        ratings: 4.8,
        reviews_count: 678,
        images: [
            "/images/keyboard1.jpg",
            "/images/keyboard2.jpg"
        ],
        specifications: {
            switch_type: "Cherry MX Blue",
            backlight: "RGB",
            layout: "Full Size",
            connection: "USB-C Wireless"
        },
        is_active: true,
        createdAt: new Date(),
        updatedAt: new Date()
    },
    {
        _id: 5,
        name: "Wireless Mouse",
        description: "Ergonomic wireless mouse with precision tracking",
        price: 59.99,
        category: "Electronics",
        stock_reference_id: 5,
        brand: "PointPerfect",
        ratings: 4.4,
        reviews_count: 423,
        images: [
            "/images/mouse1.jpg"
        ],
        specifications: {
            dpi: "16000",
            buttons: "7",
            battery_life: "18 months",
            weight: "95 grams"
        },
        is_active: true,
        createdAt: new Date(),
        updatedAt: new Date()
    }
]);

// Insert categories
db.categories.insertMany([
    {
        _id: "electronics",
        name: "Electronics",
        description: "Electronic devices and gadgets",
        icon: "📱"
    },
    {
        _id: "accessories",
        name: "Accessories",
        description: "Computer and device accessories",
        icon: "🔌"
    },
    {
        _id: "office",
        name: "Office",
        description: "Office furniture and supplies",
        icon: "📊"
    }
]);

print("MongoDB initialization completed successfully!");
