import React from 'react';
import ReactDOM from 'react-dom/client';
import H3MapVisualization from './components/H3MapVisualization';
import './styles/H3MapVisualization.css';

// Performance monitoring
const startTime = performance.now();

// Error boundary component
class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error) {
    return { hasError: true, error };
  }

  componentDidCatch(error, errorInfo) {
    console.error('React Error Boundary caught an error:', error, errorInfo);
  }

  render() {
    if (this.state.hasError) {
      return (
        <div style={{
          display: 'flex',
          flexDirection: 'column',
          justifyContent: 'center',
          alignItems: 'center',
          width: '100vw',
          height: '100vh',
          background: 'linear-gradient(135deg, #ff6b6b 0%, #ee5a24 100%)',
          color: 'white',
          fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", "Roboto", sans-serif',
          textAlign: 'center',
          padding: '2rem'
        }}>
          <div style={{
            background: 'white',
            color: '#d63031',
            padding: '2rem 3rem',
            borderRadius: '15px',
            boxShadow: '0 20px 40px rgba(0,0,0,0.1)',
            maxWidth: '500px'
          }}>
            <h1 style={{ margin: '0 0 1rem 0', fontSize: '1.5rem' }}>
              🚨 Application Error
            </h1>
            <p style={{ margin: '0 0 1rem 0', fontSize: '1rem' }}>
              Something went wrong loading the H3 visualization.
            </p>
            <details style={{ textAlign: 'left', fontSize: '0.9rem', margin: '1rem 0' }}>
              <summary style={{ cursor: 'pointer', fontWeight: 'bold' }}>
                Error Details
              </summary>
              <pre style={{ 
                background: '#f8f9fa', 
                padding: '1rem', 
                borderRadius: '5px', 
                overflow: 'auto',
                fontSize: '0.8rem',
                marginTop: '0.5rem'
              }}>
                {this.state.error?.toString()}
              </pre>
            </details>
            <button 
              onClick={() => window.location.reload()}
              style={{
                background: '#d63031',
                color: 'white',
                border: 'none',
                padding: '1rem 2rem',
                borderRadius: '10px',
                fontSize: '1rem',
                fontWeight: '600',
                cursor: 'pointer'
              }}
            >
              🔄 Reload Application
            </button>
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}

// Main app wrapper
const App = () => {
  React.useEffect(() => {
    // Log performance
    const loadTime = performance.now() - startTime;
    console.log(`🚀 App loaded in ${loadTime.toFixed(2)}ms`);
    
    // Set document title
    document.title = '🇺🇦 Kyiv H3 Retail Intelligence';
  }, []);

  return (
    <ErrorBoundary>
      <H3MapVisualization />
    </ErrorBoundary>
  );
};

// Create React root and render
const root = ReactDOM.createRoot(document.getElementById('root'));

// Render with error handling
try {
  root.render(
    <React.StrictMode>
      <App />
    </React.StrictMode>
  );
  
  // Performance logging
  if (process.env.NODE_ENV === 'development') {
    console.log('🎯 H3 Kyiv Visualization loaded successfully!');
    console.log('📊 Available in development mode');
    console.log('🔧 Backend API should be running on http://localhost:8000');
  }
  
} catch (error) {
  console.error('Failed to render React app:', error);
  
  // Fallback error display
  document.getElementById('root').innerHTML = `
    <div style="
      display: flex;
      justify-content: center;
      align-items: center;
      width: 100vw;
      height: 100vh;
      background: linear-gradient(135deg, #ff6b6b 0%, #ee5a24 100%);
      color: white;
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif;
      text-align: center;
    ">
      <div style="
        background: white;
        color: #d63031;
        padding: 2rem 3rem;
        border-radius: 15px;
        box-shadow: 0 20px 40px rgba(0,0,0,0.1);
      ">
        <h1>🚨 Critical Error</h1>
        <p>Failed to initialize React application.</p>
        <p style="font-size: 0.9rem; margin-top: 1rem;">Check console for details.</p>
        <button onclick="window.location.reload()" style="
          margin-top: 1rem;
          background: #d63031;
          color: white;
          border: none;
          padding: 1rem 2rem;
          border-radius: 10px;
          cursor: pointer;
        ">
          🔄 Reload
        </button>
      </div>
    </div>
  `;
}