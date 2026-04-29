// Common functions and utilities

const API_BASE_URL = 'http://localhost:5000/api';

// Get token from localStorage
function getToken() {
    return localStorage.getItem('token');
}

// Set token in localStorage
function setToken(token) {
    localStorage.setItem('token', token);
}

// Get user info from localStorage
function getUserInfo() {
    return JSON.parse(localStorage.getItem('user') || 'null') || null;
}

// Set user info in localStorage
function setUserInfo(user) {
    localStorage.setItem('user', JSON.stringify(user));
}

// Logout function
function logout() {
    localStorage.removeItem('token');
    localStorage.removeItem('user');
    localStorage.removeItem('cart');
    window.location.href = '/';
}

// Make API calls with authentication
async function apiCall(method, endpoint, data = null) {
    const options = {
        method: method,
        headers: {
            'Content-Type': 'application/json',
        }
    };

    const token = getToken();
    if (token) {
        options.headers['Authorization'] = `Bearer ${token}`;
    }

    if (data) {
        options.body = JSON.stringify(data);
    }

    try {
        const response = await fetch(`${API_BASE_URL}${endpoint}`, options);
        const result = await response.json();

        if (!response.ok) {
            if (response.status === 401) {
                logout();
                throw new Error('Session expired');
            }
            throw new Error(result.error || 'API request failed');
        }

        return result;
    } catch (error) {
        console.error('API Error:', error);
        throw error;
    }
}

// Update navigation based on auth status
function updateNav() {
    const token = getToken();
    const user = getUserInfo();
    const authNav = document.getElementById('auth-nav');
    const userNav = document.getElementById('user-nav');
    const adminNav = document.getElementById('admin-nav');

    if (token && user && user.user_id) {
        if (authNav) authNav.style.display = 'none';
        if (userNav) userNav.style.display = 'flex';
        if (user.is_admin && adminNav) adminNav.style.display = 'block';
    } else {
        if (authNav) authNav.style.display = 'block';
        if (userNav) userNav.style.display = 'none';
        if (adminNav) adminNav.style.display = 'none';
    }
}

function initializeMobileNav() {
    const navbar = document.querySelector('.navbar-container');
    const menu = document.querySelector('.navbar-menu');
    if (!navbar || !menu) return;

    let toggle = navbar.querySelector('.nav-toggle');
    if (!toggle) {
        toggle = document.createElement('button');
        toggle.className = 'nav-toggle';
        toggle.setAttribute('type', 'button');
        toggle.setAttribute('aria-label', 'Toggle navigation');
        toggle.innerHTML = '&#9776;';
        navbar.appendChild(toggle);
    }

    const closeMenu = () => menu.classList.remove('open');

    toggle.onclick = () => {
        menu.classList.toggle('open');
    };

    menu.querySelectorAll('a').forEach((link) => {
        link.addEventListener('click', closeMenu);
    });

    window.addEventListener('resize', () => {
        if (window.innerWidth > 860) closeMenu();
    });
}

// Show alert
function showAlert(message, type = 'info') {
    let toastRoot = document.getElementById('toast-root');
    if (!toastRoot) {
        toastRoot = document.createElement('div');
        toastRoot.id = 'toast-root';
        toastRoot.style.position = 'fixed';
        toastRoot.style.top = '1rem';
        toastRoot.style.right = '1rem';
        toastRoot.style.zIndex = '9999';
        toastRoot.style.display = 'flex';
        toastRoot.style.flexDirection = 'column';
        toastRoot.style.gap = '0.6rem';
        toastRoot.style.maxWidth = '380px';
        document.body.appendChild(toastRoot);
    }

    const alertDiv = document.createElement('div');
    alertDiv.className = `alert alert-${type}`;
    alertDiv.textContent = message;
    alertDiv.style.boxShadow = '0 8px 20px rgba(15, 23, 42, 0.12)';
    alertDiv.style.margin = '0';
    alertDiv.style.pointerEvents = 'auto';

    toastRoot.appendChild(alertDiv);

    setTimeout(() => {
        alertDiv.remove();
        if (toastRoot && toastRoot.children.length === 0) {
            toastRoot.remove();
        }
    }, 5000);
}

// Format currency
function formatCurrency(amount) {
    return new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'USD'
    }).format(amount);
}

// Format date
function formatDate(dateString) {
    const options = { year: 'numeric', month: 'long', day: 'numeric' };
    return new Date(dateString).toLocaleDateString(undefined, options);
}

// Cart management
class Cart {
    constructor() {
        this.items = JSON.parse(localStorage.getItem('cart') || '[]');
    }

    addItem(product) {
        const existingItem = this.items.find(item => item.product_id === product._id);
        
        if (existingItem) {
            existingItem.quantity += product.quantity || 1;
        } else {
            this.items.push({
                product_id: product._id,
                product_name: product.name,
                price: product.price,
                quantity: product.quantity || 1
            });
        }
        
        this.save();
    }

    removeItem(productId) {
        this.items = this.items.filter(item => item.product_id !== productId);
        this.save();
    }

    updateQuantity(productId, quantity) {
        const item = this.items.find(item => item.product_id === productId);
        if (item) {
            item.quantity = Math.max(1, quantity);
            this.save();
        }
    }

    clear() {
        this.items = [];
        this.save();
    }

    getTotal() {
        return this.items.reduce((total, item) => total + (item.price * item.quantity), 0);
    }

    getItemCount() {
        return this.items.reduce((count, item) => count + item.quantity, 0);
    }

    save() {
        localStorage.setItem('cart', JSON.stringify(this.items));
    }
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', function() {
    updateNav();
    initializeMobileNav();
});
