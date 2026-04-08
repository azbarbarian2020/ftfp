// Prediction polling patch for v51 frontend
// This script adds the missing prediction polling that should have been in the React app

(function() {
  console.log('✅ Prediction polling patch loaded');
  
  let lastPredictionData = null;
  
  async function pollPredictions() {
    try {
      const response = await fetch('/api/predictions/latest?_t=' + Date.now());
      const data = await response.json();
      
      // Check if data changed by comparing JSON strings
      const dataString = JSON.stringify(data);
      if (dataString !== lastPredictionData) {
        console.log('🔄 Predictions updated - detected change');
        lastPredictionData = dataString;
        
        // Trigger a page reload to show new data
        // This is the simplest way to force React to re-render with new data
        window.location.reload();
      }
    } catch (error) {
      console.error('Prediction polling error:', error);
    }
  }
  
  // Start polling after page loads
  window.addEventListener('load', function() {
    // Wait 10 seconds before starting to poll (let initial data load)
    setTimeout(function() {
      console.log('✅ Starting prediction polling (every 5 seconds)');
      setInterval(pollPredictions, 5000);
    }, 10000);
  });
})();

