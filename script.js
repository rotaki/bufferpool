// Import the init function and the module class
import init, { FosterBtreeVisualizer } from './pkg/bp.js';

// First, ensure the module is initialized before using it
init().then(() => {
    // Module is initialized, now you can create an instance of FosterBtreeVisualizer
    const fosterTree = new FosterBtreeVisualizer();

    // Set up event listeners for insert and search actions
    setupInsert(fosterTree);
    setupSearch(fosterTree);
    setupClearLogs();
}).catch(error => {
    console.error('Error initializing the WebAssembly module:', error);
});

function setupInsert(fosterTree) {
    const insertButton = document.getElementById('insertButton');
    insertButton.addEventListener('click', () => {
        const key = document.getElementById('insertKey').value;
        const payloadSize = document.getElementById('payloadSize').value;
        const result = fosterTree.insert(key, payloadSize);
        const message = `Insert operation performed on key: ${key} with payload size: ${payloadSize}, result: ${result}`;
        logOperation(fosterTree, message);
    });
}

function setupSearch(fosterTree) {
    const searchButton = document.getElementById('searchButton');
    searchButton.addEventListener('click', () => {
        const key = document.getElementById('searchKey').value;
        const result = fosterTree.search(key);
        const message = `Search operation performed on key: ${key}, result: ${result}`;
        logOperation(fosterTree, message);
    });
}

function logOperation(fosterTree, message) {
    console.log(message); // Print to console log

    // Append the operation message to the container
    const container = document.getElementById('contentContainer');
    const logEntry = document.createElement('div');
    logEntry.textContent = message;
    container.appendChild(logEntry);

    // Visualize the tree and append the graph
    const dotString = fosterTree.visualize(); // Assuming visualize returns a DOT representation of the tree
    const graphContainer = document.createElement('div');
    container.appendChild(graphContainer);
    d3.select(graphContainer).graphviz().renderDot(dotString);
}

function setupClearLogs() {
    const clearLogsButton = document.getElementById('clearLogsButton');
    clearLogsButton.addEventListener('click', () => {
        const container = document.getElementById('contentContainer');
        if (container.children.length > 2) {
            const secondToLastElement = container.children[container.children.length - 2];
            const lastElement = container.lastElementChild;
            container.innerHTML = ''; // Clear all children
            container.appendChild(secondToLastElement); // Re-append the second to last child
            container.appendChild(lastElement); // Re-append the last child
        }
    });
}
