// Home page - Product listing

let currentPage = 1;
const itemsPerPage = 20;
let allProducts = [];

async function loadProducts(page = 1, search = '', category = '') {
    try {
        const params = new URLSearchParams({
            page: page,
            limit: itemsPerPage,
            ...(category && { category }),
            ...(search && { search })
        });

        let endpoint = `/products?${params.toString()}`;
        
        const response = await fetch(`${API_BASE_URL}${endpoint}`);
        const result = await response.json();

        if (result.status === 'success') {
            displayProducts(result.data.products);
            currentPage = page;
            return result.data;
        } else {
            showAlert('Failed to load products', 'error');
        }
    } catch (error) {
        console.error('Error loading products:', error);
        showAlert('Error loading products', 'error');
    }
}

function displayProducts(products) {
    const container = document.getElementById('products-container');
    
    if (!products || products.length === 0) {
        container.innerHTML = '<p style="grid-column: 1/-1; text-align: center; padding: 2rem;">No products found</p>';
        return;
    }

    container.innerHTML = products.map(product => `
        <div class="product-card" onclick="viewProduct(${product._id})">
            <div class="product-image">
                📦
            </div>
            <div class="product-content">
                <div class="product-name">${product.name}</div>
                <div class="product-description">${product.description.substring(0, 60)}...</div>
                <div class="product-footer">
                    <div>
                        <div class="product-price">${formatCurrency(product.price)}</div>
                        <div class="product-rating">★ ${product.ratings || 4.5}</div>
                    </div>
                    <button class="product-btn" onclick="addToCartModal(event, ${product._id}, '${product.name}', ${product.price})">Add</button>
                </div>
            </div>
        </div>
    `).join('');
}

function viewProduct(productId) {
    window.location.href = `/product.html?id=${productId}`;
}

function addToCartModal(event, productId, productName, price) {
    event.stopPropagation();
    
    const quantity = prompt(`Add "${productName}" to cart?\n\nEnter quantity:`, '1');
    
    if (quantity && !isNaN(quantity) && quantity > 0) {
        const cart = new Cart();
        cart.addItem({
            _id: productId,
            name: productName,
            price: price,
            quantity: parseInt(quantity)
        });
        
        showAlert(`${productName} added to cart!`, 'success');
    }
}

// Search functionality
document.addEventListener('DOMContentLoaded', function() {
    const searchBox = document.getElementById('search-box');
    const categoryFilter = document.getElementById('category-filter');

    // Load initial products
    loadProducts();

    // Search
    if (searchBox) {
        searchBox.addEventListener('input', debounce(function() {
            const search = this.value;
            const category = categoryFilter.value;
            loadProducts(1, search, category);
        }, 500));
    }

    // Category filter
    if (categoryFilter) {
        categoryFilter.addEventListener('change', function() {
            const category = this.value;
            const search = searchBox.value;
            loadProducts(1, search, category);
        });
    }
});

function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func.apply(this, args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}
