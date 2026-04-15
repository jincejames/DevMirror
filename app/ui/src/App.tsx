import { Component, type ReactNode } from 'react';
import { Routes, Route, Link, useLocation } from 'react-router-dom';
import ConfigList from './pages/ConfigList';
import ConfigForm from './pages/ConfigForm';
import ScanResults from './pages/ScanResults';
import ProvisionProgress from './pages/ProvisionProgress';
import DrList from './pages/DrList';
import DrStatus from './pages/DrStatus';

class ErrorBoundary extends Component<
  { children: ReactNode },
  { error: string | null }
> {
  state = { error: null as string | null };

  static getDerivedStateFromError(err: Error) {
    return { error: err.message };
  }

  render() {
    if (this.state.error) {
      return (
        <div style={{ padding: 20, color: 'red' }}>
          <h2>Something went wrong</h2>
          <pre>{this.state.error}</pre>
          <button onClick={() => (window.location.href = '/')}>
            Go Home
          </button>
        </div>
      );
    }
    return this.props.children;
  }
}

function NavHeader() {
  const location = useLocation();
  const isConfigs = location.pathname === '/' || location.pathname.startsWith('/config');
  const isDrs = location.pathname.startsWith('/dr');
  return (
    <header className="app-header">
      <Link to="/" className="app-title">DevMirror</Link>
      <nav className="app-nav">
        <Link to="/" className={isConfigs ? 'active' : ''}>Configs</Link>
        <Link to="/drs" className={isDrs ? 'active' : ''}>Active DRs</Link>
      </nav>
    </header>
  );
}

export default function App() {
  return (
    <div className="app">
      <NavHeader />
      <main className="app-main">
        <ErrorBoundary>
          <Routes>
            <Route path="/" element={<ConfigList />} />
            <Route path="/config/new" element={<ConfigForm />} />
            <Route path="/config/:drId" element={<ConfigForm />} />
            <Route path="/config/:drId/scan" element={<ScanResults />} />
            <Route path="/config/:drId/provision/:taskId" element={<ProvisionProgress />} />
            <Route path="/drs" element={<DrList />} />
            <Route path="/dr/:drId" element={<DrStatus />} />
          </Routes>
        </ErrorBoundary>
      </main>
    </div>
  );
}
