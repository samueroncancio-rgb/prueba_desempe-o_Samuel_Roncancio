import React, { useState, useEffect, useRef } from 'react';
import { 
  Activity, Shield, AlertTriangle, Database, Bot, RefreshCw, 
  Download, LogOut, CheckCircle2, XCircle, Search, Filter, 
  MapPin, Send, Server, Sparkles, User, Info, ArrowRight
} from 'lucide-react';

const API_BASE = 'http://localhost:8000';

interface UserSession {
  username: string;
  role: 'admin' | 'viewer';
  token: string;
}

export default function App() {
  const [session, setSession] = useState<UserSession | null>(() => {
    const saved = localStorage.getItem('sismowatch_session');
    return saved ? JSON.parse(saved) : null;
  });

  const [activeTab, setActiveTab] = useState<'dashboard' | 'reports' | 'pipeline' | 'admin'>('dashboard');
  const [kpis, setKpis] = useState<any>(null);
  const [events, setEvents] = useState<any[]>([]);
  const [reports, setReports] = useState<any[]>([]);
  const [pipelineStatus, setPipelineStatus] = useState<any>(null);
  const [quarantineItems, setQuarantineItems] = useState<any[]>([]);
  
  // Real-time SSE state
  const [isLiveConnected, setIsLiveConnected] = useState<boolean>(false);
  const [lastSyncTime, setLastSyncTime] = useState<string>(new Date().toLocaleTimeString());

  // Filters
  const [minMag, setMinMag] = useState<number>(0);
  const [regionFilter, setRegionFilter] = useState<string>('');
  const [loading, setLoading] = useState<boolean>(false);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);

  // RAG Chatbot Drawer
  const [chatOpen, setChatOpen] = useState<boolean>(false);
  const [chatMessages, setChatMessages] = useState<Array<{ sender: 'user' | 'bot'; text: string; sources?: any[] }>>([
    { sender: 'bot', text: '¡Hola! Soy SismoBot, el agente de IA de SismoWatch. Puedo responder tus preguntas sobre sismos recientes, alertas de tsunami, anomalías espaciales y el pipeline Lakehouse.' }
  ]);
  const [inputMessage, setInputMessage] = useState<string>('');
  const [chatLoading, setChatLoading] = useState<boolean>(false);

  // Login Form State
  const [loginUsername, setLoginUsername] = useState('admin@sismowatch.com');
  const [loginPassword, setLoginPassword] = useState('AdminSecurePass2026!');
  const [loginError, setLoginError] = useState<string | null>(null);

  // Leaflet map ref
  const mapRef = useRef<any>(null);

  const saveSession = (sess: UserSession | null) => {
    setSession(sess);
    if (sess) {
      localStorage.setItem('sismowatch_session', JSON.stringify(sess));
    } else {
      localStorage.removeItem('sismowatch_session');
    }
  };

  const handleLogin = async (e?: React.FormEvent) => {
    if (e) e.preventDefault();
    setLoginError(null);
    try {
      const res = await fetch(`${API_BASE}/auth/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: loginUsername, password: loginPassword })
      });
      if (!res.ok) {
        throw new Error('Credenciales inválidas. Por favor intente de nuevo.');
      }
      const data = await res.json();
      saveSession({ username: data.username, role: data.role, token: data.access_token });
    } catch (err: any) {
      setLoginError(err.message || 'Error de conexión con la API');
    }
  };

  const handleLogout = () => {
    saveSession(null);
  };

  // Fetch Dashboard Data
  const fetchData = async () => {
    if (!session) return;
    setLoading(true);
    setErrorMsg(null);
    try {
      const headers = { Authorization: `Bearer ${session.token}` };
      
      // KPIs
      const kpiRes = await fetch(`${API_BASE}/api/kpis`, { headers });
      if (kpiRes.ok) setKpis(await kpiRes.json());

      // Events with filters
      let eventsUrl = `${API_BASE}/api/events?limit=50`;
      if (minMag > 0) eventsUrl += `&min_mag=${minMag}`;
      if (regionFilter) eventsUrl += `&region=${encodeURIComponent(regionFilter)}`;
      const evRes = await fetch(eventsUrl, { headers });
      if (evRes.ok) {
        const evData = await evRes.json();
        setEvents(evData.items || []);
      }

      // Reports
      const repRes = await fetch(`${API_BASE}/api/reports?limit=10`, { headers });
      if (repRes.ok) setReports(await repRes.json());

      // Pipeline Status
      const pipeRes = await fetch(`${API_BASE}/api/pipeline/status`, { headers });
      if (pipeRes.ok) setPipelineStatus(await pipeRes.json());

      // Quarantine (only if admin)
      if (session.role === 'admin') {
        const quarRes = await fetch(`${API_BASE}/api/quality/quarantine`, { headers });
        if (quarRes.ok) setQuarantineItems(await quarRes.json());
      }

      setLastSyncTime(new Date().toLocaleTimeString());
    } catch (err: any) {
      setErrorMsg('No se pudo sincronizar la telemetría. Asegúrese de que la API esté corriendo.');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (session) {
      fetchData();
    }
  }, [session, minMag, regionFilter]);

  // Server-Sent Events (SSE) Real-Time Hook
  useEffect(() => {
    if (!session) return;
    let eventSource: EventSource | null = null;
    let reconnectTimeout: any = null;

    const connectSSE = () => {
      // EventSource with bearer token in query or custom header if supported
      eventSource = new EventSource(`${API_BASE}/api/stream?token=${session.token}`);
      
      eventSource.addEventListener('connect', () => {
        setIsLiveConnected(true);
      });

      eventSource.addEventListener('seismic_update', (e: any) => {
        try {
          const newBatch = JSON.parse(e.data);
          if (Array.isArray(newBatch) && newBatch.length > 0) {
            setLastSyncTime(new Date().toLocaleTimeString());
            // Prepend new events
            setEvents(prev => {
              const ids = new Set(prev.map(item => item.event_id));
              const filtered = newBatch.filter((item: any) => !ids.has(item.event_id));
              return [...filtered, ...prev].slice(0, 50);
            });
          }
        } catch (err) {}
      });

      eventSource.addEventListener('heartbeat', () => {
        setIsLiveConnected(true);
      });

      eventSource.onerror = () => {
        setIsLiveConnected(false);
        if (eventSource) eventSource.close();
        reconnectTimeout = setTimeout(connectSSE, 5000);
      };
    };

    connectSSE();

    return () => {
      if (eventSource) eventSource.close();
      if (reconnectTimeout) clearTimeout(reconnectTimeout);
    };
  }, [session]);

  // Initialize Leaflet Map
  useEffect(() => {
    if (activeTab === 'dashboard' && events.length > 0 && (window as any).L) {
      const L = (window as any).L;
      const mapContainer = document.getElementById('map-container');
      if (mapContainer) {
        if (!mapRef.current) {
          mapRef.current = L.map('map-container').setView([20, 0], 2);
          L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
            attribution: '&copy; OpenStreetMap contributors &copy; CARTO'
          }).addTo(mapRef.current);
        }

        // Clear existing markers
        mapRef.current.eachLayer((layer: any) => {
          if (layer instanceof L.CircleMarker) {
            mapRef.current.removeLayer(layer);
          }
        });

        // Add markers
        events.forEach(ev => {
          const color = ev.mag >= 5.5 ? '#ef4444' : ev.mag >= 4.0 ? '#f59e0b' : '#10b981';
          const radius = Math.max(4, ev.mag * 2.5);
          L.circleMarker([ev.latitude, ev.longitude], {
            radius: radius,
            fillColor: color,
            color: '#fff',
            weight: 1,
            opacity: 0.9,
            fillOpacity: 0.7
          })
          .bindPopup(`
            <div style="color: #1e293b; font-size: 12px;">
              <strong>${ev.place_sanitized}</strong><br/>
              <b>Magnitud:</b> ${ev.mag} M<br/>
              <b>Profundidad:</b> ${ev.depth_km} km<br/>
              <b>Alerta Tsunami:</b> ${ev.tsunami_alert ? 'Sí' : 'No'}<br/>
              <b>Estado:</b> ${ev.is_auto_healed ? 'Auto-Curado' : 'Validado'}<br/>
              <small>${new Date(ev.event_time).toLocaleString()}</small>
            </div>
          `)
          .addTo(mapRef.current);
        });
      }
    }
  }, [activeTab, events]);

  // Send RAG Chat Message
  const handleSendMessage = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!inputMessage.trim() || !session) return;
    const msg = inputMessage.trim();
    setInputMessage('');
    setChatMessages(prev => [...prev, { sender: 'user', text: msg }]);
    setChatLoading(true);

    try {
      const res = await fetch(`${API_BASE}/api/agent/chat`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${session.token}`
        },
        body: JSON.stringify({ message: msg })
      });
      const data = await res.json();
      setChatMessages(prev => [
        ...prev, 
        { sender: 'bot', text: data.reply, sources: data.sources }
      ]);
    } catch (err) {
      setChatMessages(prev => [
        ...prev, 
        { sender: 'bot', text: 'Error al contactar con el agente de IA. Intente de nuevo.' }
      ]);
    } finally {
      setChatLoading(false);
    }
  };

  // Admin trigger backfill
  const handleTriggerBackfill = async () => {
    if (!session || session.role !== 'admin') return;
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE}/api/admin/backfill`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${session.token}`
        },
        body: JSON.stringify({ days: 30, min_magnitude: 2.5 })
      });
      const data = await res.json();
      alert(`Backfill completado: ${data.message}`);
      fetchData();
    } catch (e: any) {
      alert('Fallo al ejecutar backfill: ' + e.message);
    } finally {
      setLoading(false);
    }
  };

  // IF NOT LOGGED IN
  if (!session) {
    return (
      <div className="min-h-screen flex items-center justify-center p-4 bg-gradient-to-br from-slate-950 via-slate-900 to-emerald-950">
        <div className="max-w-md w-full bg-slate-800/80 backdrop-blur-md border border-slate-700/60 rounded-2xl p-8 shadow-2xl">
          <div className="flex items-center space-x-3 mb-6">
            <div className="p-3 bg-emerald-500/20 text-emerald-400 rounded-xl border border-emerald-500/30">
              <Activity className="w-8 h-8" />
            </div>
            <div>
              <h1 className="text-2xl font-bold text-white tracking-tight">SismoWatch</h1>
              <p className="text-xs text-emerald-400 font-medium">Enterprise Seismic Intelligence • SuperApp</p>
            </div>
          </div>

          <form onSubmit={handleLogin} className="space-y-4">
            <div>
              <label className="block text-xs font-semibold text-slate-300 uppercase tracking-wider mb-1.5">Usuario / Correo</label>
              <input 
                type="text" 
                value={loginUsername}
                onChange={e => setLoginUsername(e.target.value)}
                className="w-full px-4 py-2.5 bg-slate-900/80 border border-slate-700 rounded-lg text-white focus:outline-none focus:border-emerald-500 transition text-sm"
              />
            </div>
            <div>
              <label className="block text-xs font-semibold text-slate-300 uppercase tracking-wider mb-1.5">Contraseña</label>
              <input 
                type="password" 
                value={loginPassword}
                onChange={e => setLoginPassword(e.target.value)}
                className="w-full px-4 py-2.5 bg-slate-900/80 border border-slate-700 rounded-lg text-white focus:outline-none focus:border-emerald-500 transition text-sm"
              />
            </div>

            {loginError && (
              <div className="p-3 bg-red-500/10 border border-red-500/30 rounded-lg text-red-400 text-xs">
                {loginError}
              </div>
            )}

            <button
              type="submit"
              className="w-full py-3 bg-emerald-600 hover:bg-emerald-500 text-white font-semibold rounded-lg shadow-lg shadow-emerald-900/30 transition flex items-center justify-center space-x-2 text-sm"
            >
              <span>Ingresar a la Plataforma</span>
              <ArrowRight className="w-4 h-4" />
            </button>
          </form>

          <div className="mt-6 pt-6 border-t border-slate-700/60">
            <p className="text-xs text-slate-400 mb-3 text-center">Acceso Rápido de Prueba (Demo Roles):</p>
            <div className="grid grid-cols-2 gap-2">
              <button 
                onClick={() => {
                  setLoginUsername('admin@sismowatch.com');
                  setLoginPassword('AdminSecurePass2026!');
                }}
                className="px-3 py-2 bg-slate-700/50 hover:bg-slate-700 text-slate-200 rounded text-xs transition border border-slate-600/40 text-center"
              >
                <b>Admin</b> (Control Total)
              </button>
              <button 
                onClick={() => {
                  setLoginUsername('viewer@sismowatch.com');
                  setLoginPassword('ViewerPass2026!');
                }}
                className="px-3 py-2 bg-slate-700/50 hover:bg-slate-700 text-slate-200 rounded text-xs transition border border-slate-600/40 text-center"
              >
                <b>Viewer</b> (Solo Lectura)
              </button>
            </div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-slate-950 flex flex-col">
      {/* Navigation Bar */}
      <header className="bg-slate-900/80 backdrop-blur-md border-b border-slate-800 sticky top-0 z-40 px-6 py-3.5 flex items-center justify-between">
        <div className="flex items-center space-x-3">
          <div className="p-2 bg-emerald-500/20 text-emerald-400 rounded-lg border border-emerald-500/30">
            <Activity className="w-6 h-6" />
          </div>
          <div>
            <div className="flex items-center space-x-2">
              <span className="font-bold text-lg text-white">SismoWatch</span>
              <span className="text-xs px-2 py-0.5 bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 rounded-full font-mono">
                Lakehouse Medallion
              </span>
            </div>
            <p className="text-xs text-slate-400">SuperApp Global Monitoring • Capa de Servicio Gold</p>
          </div>
        </div>

        {/* Real-time Indicator & Controls */}
        <div className="flex items-center space-x-4">
          <div className="flex items-center space-x-2 px-3 py-1 bg-slate-800/80 border border-slate-700 rounded-full text-xs">
            <span className={`w-2 h-2 rounded-full ${isLiveConnected ? 'bg-emerald-400 animate-pulse' : 'bg-amber-400'}`} />
            <span className="text-slate-300 font-medium">{isLiveConnected ? 'EN VIVO (SSE)' : 'Reconectando...'}</span>
            <span className="text-slate-500 font-mono">({lastSyncTime})</span>
          </div>

          <button
            onClick={fetchData}
            disabled={loading}
            className="p-2 text-slate-400 hover:text-white bg-slate-800 hover:bg-slate-700 rounded-lg transition"
            title="Sincronizar ahora"
          >
            <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin text-emerald-400' : ''}`} />
          </button>

          <button
            onClick={() => setChatOpen(!chatOpen)}
            className="flex items-center space-x-1.5 px-3 py-1.5 bg-indigo-600/30 hover:bg-indigo-600/40 text-indigo-300 border border-indigo-500/40 rounded-lg text-xs font-semibold transition"
          >
            <Bot className="w-4 h-4" />
            <span>SismoBot RAG</span>
          </button>

          {/* User profile badge */}
          <div className="flex items-center space-x-2 pl-2 border-l border-slate-800">
            <div className="text-right">
              <div className="text-xs font-semibold text-white">{session.username.split('@')[0]}</div>
              <span className={`text-[10px] px-1.5 py-0.2 rounded font-mono uppercase font-bold ${session.role === 'admin' ? 'bg-purple-500/20 text-purple-300' : 'bg-blue-500/20 text-blue-300'}`}>
                {session.role}
              </span>
            </div>
            <button
              onClick={handleLogout}
              className="p-1.5 text-slate-400 hover:text-red-400 hover:bg-slate-800 rounded transition"
              title="Cerrar sesión"
            >
              <LogOut className="w-4 h-4" />
            </button>
          </div>
        </div>
      </header>

      {/* Main Container */}
      <div className="flex-1 flex overflow-hidden">
        {/* Sidebar Navigation */}
        <aside className="w-60 bg-slate-900 border-r border-slate-800 p-4 space-y-1">
          <button
            onClick={() => setActiveTab('dashboard')}
            className={`w-full flex items-center space-x-2.5 px-3 py-2.5 rounded-lg text-xs font-medium transition ${activeTab === 'dashboard' ? 'bg-emerald-600 text-white font-semibold' : 'text-slate-400 hover:bg-slate-800 hover:text-white'}`}
          >
            <Activity className="w-4 h-4" />
            <span>Dashboard y Mapa</span>
          </button>

          <button
            onClick={() => setActiveTab('reports')}
            className={`w-full flex items-center space-x-2.5 px-3 py-2.5 rounded-lg text-xs font-medium transition ${activeTab === 'reports' ? 'bg-emerald-600 text-white font-semibold' : 'text-slate-400 hover:bg-slate-800 hover:text-white'}`}
          >
            <Sparkles className="w-4 h-4" />
            <span>Reportes Diarios IA</span>
          </button>

          <button
            onClick={() => setActiveTab('pipeline')}
            className={`w-full flex items-center space-x-2.5 px-3 py-2.5 rounded-lg text-xs font-medium transition ${activeTab === 'pipeline' ? 'bg-emerald-600 text-white font-semibold' : 'text-slate-400 hover:bg-slate-800 hover:text-white'}`}
          >
            <Server className="w-4 h-4" />
            <span>Salud del Pipeline</span>
          </button>

          {session.role === 'admin' && (
            <button
              onClick={() => setActiveTab('admin')}
              className={`w-full flex items-center space-x-2.5 px-3 py-2.5 rounded-lg text-xs font-medium transition ${activeTab === 'admin' ? 'bg-purple-600 text-white font-semibold' : 'text-purple-400 hover:bg-slate-800 hover:text-purple-300'}`}
            >
              <Shield className="w-4 h-4" />
              <span>Panel Admin & Cuarentena</span>
            </button>
          )}

          <div className="pt-6 mt-6 border-t border-slate-800/80">
            <div className="p-3 bg-slate-800/50 rounded-lg border border-slate-700/40">
              <div className="flex items-center space-x-1.5 text-xs font-semibold text-emerald-400 mb-1">
                <Database className="w-3.5 h-3.5" />
                <span>Base de Datos Limpia</span>
              </div>
              <p className="text-[11px] text-slate-400 leading-relaxed">
                PostgreSQL contiene estrictamente los <b>hechos curados de GOLD</b>. Bronze y Silver residen en MinIO.
              </p>
            </div>
          </div>
        </aside>

        {/* Content Area */}
        <main className="flex-1 overflow-y-auto p-6 space-y-6">
          {errorMsg && (
            <div className="p-4 bg-amber-500/10 border border-amber-500/30 rounded-xl text-amber-300 text-sm flex items-center justify-between">
              <span>{errorMsg}</span>
              <button onClick={fetchData} className="underline text-xs hover:text-white">Reintentar</button>
            </div>
          )}

          {/* TAB 1: DASHBOARD */}
          {activeTab === 'dashboard' && (
            <div className="space-y-6">
              {/* KPIs Bar */}
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-6 gap-4">
                <div className="p-4 bg-slate-900 border border-slate-800 rounded-xl">
                  <span className="text-xs text-slate-400 font-medium">Eventos 24h</span>
                  <div className="text-2xl font-bold text-white mt-1">{kpis ? kpis.total_events_24h : '...'}</div>
                  <span className="text-[10px] text-emerald-400">Telemetría global</span>
                </div>

                <div className="p-4 bg-slate-900 border border-slate-800 rounded-xl">
                  <span className="text-xs text-slate-400 font-medium">Magnitud Máxima</span>
                  <div className="text-2xl font-bold text-amber-400 mt-1">{kpis ? `${kpis.max_magnitude} M` : '...'}</div>
                  <span className="text-[10px] text-slate-400 truncate block">{kpis ? kpis.max_mag_place : ''}</span>
                </div>

                <div className="p-4 bg-slate-900 border border-slate-800 rounded-xl">
                  <span className="text-xs text-slate-400 font-medium">Promedio Magnitud</span>
                  <div className="text-2xl font-bold text-white mt-1">{kpis ? `${kpis.avg_magnitude} M` : '...'}</div>
                  <span className="text-[10px] text-slate-400">Media móvil</span>
                </div>

                <div className="p-4 bg-slate-900 border border-slate-800 rounded-xl">
                  <span className="text-xs text-slate-400 font-medium">Alertas Tsunami</span>
                  <div className="text-2xl font-bold text-red-400 mt-1">{kpis ? kpis.tsunami_alerts : '...'}</div>
                  <span className="text-[10px] text-slate-400">Prioridad operacional</span>
                </div>

                <div className="p-4 bg-slate-900 border border-slate-800 rounded-xl">
                  <span className="text-xs text-slate-400 font-medium">Score de Calidad</span>
                  <div className="text-2xl font-bold text-emerald-400 mt-1">{kpis ? `${kpis.quality_score}%` : '...'}</div>
                  <span className="text-[10px] text-emerald-400">Zero-Quarantine</span>
                </div>

                <div className="p-4 bg-slate-900 border border-slate-800 rounded-xl">
                  <span className="text-xs text-slate-400 font-medium">Última Carga</span>
                  <div className="text-sm font-semibold text-slate-200 mt-2 truncate">
                    {kpis && kpis.last_updated ? new Date(kpis.last_updated).toLocaleTimeString() : '...'}
                  </div>
                  <span className="text-[10px] text-slate-500">Airflow TaskFlow</span>
                </div>
              </div>

              {/* Map & Filters Section */}
              <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                <div className="lg:col-span-2 bg-slate-900 border border-slate-800 rounded-xl p-4">
                  <div className="flex items-center justify-between mb-3">
                    <div className="flex items-center space-x-2">
                      <MapPin className="w-4 h-4 text-emerald-400" />
                      <h2 className="text-sm font-bold text-white">Mapa Geoespacial de Sismos en Tiempo Real</h2>
                    </div>
                    <span className="text-xs text-slate-400">{events.length} sismos proyectados</span>
                  </div>
                  <div id="map-container" className="h-80 w-full rounded-lg overflow-hidden bg-slate-950 border border-slate-800" />
                </div>

                {/* Filter Controls Card */}
                <div className="bg-slate-900 border border-slate-800 rounded-xl p-4 space-y-4">
                  <div className="flex items-center space-x-2">
                    <Filter className="w-4 h-4 text-emerald-400" />
                    <h3 className="text-sm font-bold text-white">Filtros Operativos</h3>
                  </div>

                  <div>
                    <label className="text-xs text-slate-400 font-medium block mb-1">
                      Magnitud Mínima: <span className="text-white font-bold">{minMag} M</span>
                    </label>
                    <input 
                      type="range" 
                      min="0" 
                      max="8" 
                      step="0.5" 
                      value={minMag}
                      onChange={e => setMinMag(parseFloat(e.target.value))}
                      className="w-full accent-emerald-500 cursor-pointer"
                    />
                  </div>

                  <div>
                    <label className="text-xs text-slate-400 font-medium block mb-1">Buscar por Región</label>
                    <div className="relative">
                      <Search className="w-3.5 h-3.5 absolute left-3 top-3 text-slate-500" />
                      <input 
                        type="text" 
                        placeholder="Ej. Chile, California, Japón..." 
                        value={regionFilter}
                        onChange={e => setRegionFilter(e.target.value)}
                        className="w-full pl-9 pr-3 py-2 bg-slate-950 border border-slate-800 rounded-lg text-xs text-white focus:outline-none focus:border-emerald-500"
                      />
                    </div>
                  </div>

                  <div className="pt-4 border-t border-slate-800">
                    <a
                      href={`${API_BASE}/api/reports/export?format=csv`}
                      className="w-full py-2 bg-slate-800 hover:bg-slate-700 text-slate-200 rounded-lg text-xs font-semibold flex items-center justify-center space-x-2 transition border border-slate-700"
                    >
                      <Download className="w-3.5 h-3.5" />
                      <span>Exportar Telemetría Curada (CSV)</span>
                    </a>
                  </div>
                </div>
              </div>

              {/* Curated Events Table */}
              <div className="bg-slate-900 border border-slate-800 rounded-xl p-4">
                <div className="flex items-center justify-between mb-4">
                  <h3 className="text-sm font-bold text-white">Hechos Limpios en GOLD (fact_earthquake_events)</h3>
                  <span className="text-xs text-emerald-400 font-mono">Deduplicado por UPSERT</span>
                </div>

                <div className="overflow-x-auto">
                  <table className="w-full text-left text-xs">
                    <thead className="bg-slate-950 text-slate-400 uppercase tracking-wider text-[10px]">
                      <tr>
                        <th className="p-3">Evento ID</th>
                        <th className="p-3">Magnitud</th>
                        <th className="p-3">Lugar Sanitizado</th>
                        <th className="p-3">Profundidad</th>
                        <th className="p-3">Tsunami</th>
                        <th className="p-3">Fecha y Hora</th>
                        <th className="p-3">Auto-Curado</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-800/60 text-slate-300">
                      {events.map((ev: any) => (
                        <tr key={ev.event_id} className="hover:bg-slate-800/40 transition">
                          <td className="p-3 font-mono text-emerald-400">{ev.event_id}</td>
                          <td className="p-3 font-bold">
                            <span className={`px-2 py-0.5 rounded text-[11px] ${ev.mag >= 5.0 ? 'bg-red-500/20 text-red-400 font-black' : 'bg-slate-800 text-white'}`}>
                              {ev.mag} {ev.mag_type}
                            </span>
                          </td>
                          <td className="p-3 font-medium text-white">{ev.place_sanitized}</td>
                          <td className="p-3 font-mono">{ev.depth_km} km</td>
                          <td className="p-3">
                            {ev.tsunami_alert ? (
                              <span className="text-red-400 font-bold flex items-center space-x-1">
                                <AlertTriangle className="w-3.5 h-3.5" /> <span>ALERTA</span>
                              </span>
                            ) : (
                              <span className="text-slate-500">No</span>
                            )}
                          </td>
                          <td className="p-3 text-slate-400">{new Date(ev.event_time).toLocaleString()}</td>
                          <td className="p-3">
                            {ev.is_auto_healed ? (
                              <span className="px-2 py-0.5 bg-emerald-500/20 text-emerald-300 border border-emerald-500/30 rounded text-[10px] font-mono">
                                {ev.healing_action || 'Corregido en Staging'}
                              </span>
                            ) : (
                              <span className="text-slate-500 text-[10px]">Limpio</span>
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {events.length === 0 && (
                    <div className="p-8 text-center text-slate-500 text-xs">
                      No se encontraron eventos con los filtros actuales.
                    </div>
                  )}
                </div>
              </div>
            </div>
          )}

          {/* TAB 2: AI REPORTS */}
          {activeTab === 'reports' && (
            <div className="space-y-6">
              <div className="flex items-center justify-between">
                <div>
                  <h2 className="text-lg font-bold text-white">Reportes Ejecutivos Diarios de IA</h2>
                  <p className="text-xs text-slate-400">Generación gobernada sin alucinaciones con validación numérica determinista.</p>
                </div>
                {session.role === 'admin' && (
                  <button
                    onClick={async () => {
                      setLoading(true);
                      await fetch(`${API_BASE}/api/admin/reports/regenerate`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${session.token}` },
                        body: JSON.stringify({})
                      });
                      fetchData();
                    }}
                    className="px-3 py-2 bg-emerald-600 hover:bg-emerald-500 text-white rounded-lg text-xs font-semibold flex items-center space-x-1.5 transition"
                  >
                    <RefreshCw className="w-3.5 h-3.5" />
                    <span>Regenerar Reporte Hoy</span>
                  </button>
                )}
              </div>

              <div className="grid grid-cols-1 gap-4">
                {reports.map((rep: any) => (
                  <div key={rep.report_id} className="bg-slate-900 border border-slate-800 rounded-xl p-5 space-y-3">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center space-x-2">
                        <Sparkles className="w-4 h-4 text-emerald-400" />
                        <h3 className="text-base font-bold text-white">{rep.title}</h3>
                      </div>
                      <div className="flex items-center space-x-2">
                        {rep.llm_metrics_verified && (
                          <span className="px-2 py-0.5 bg-emerald-500/20 text-emerald-400 border border-emerald-500/30 rounded text-[11px] font-mono flex items-center space-x-1">
                            <CheckCircle2 className="w-3 h-3" />
                            <span>Cifras Verificadas Determinísticamente</span>
                          </span>
                        )}
                        {rep.is_fallback && (
                          <span className="px-2 py-0.5 bg-amber-500/20 text-amber-300 border border-amber-500/30 rounded text-[11px] font-mono">
                            Plan Alterno (Template Fallback)
                          </span>
                        )}
                      </div>
                    </div>

                    <p className="text-sm text-slate-300 leading-relaxed bg-slate-950 p-4 rounded-lg border border-slate-800">
                      {rep.summary_narrative}
                    </p>

                    <div className="flex flex-wrap gap-3 pt-2 text-xs text-slate-400">
                      <span><b>Total Eventos:</b> {rep.calculated_metrics?.total_events}</span>
                      <span>•</span>
                      <span><b>Sismo Máximo:</b> {rep.calculated_metrics?.max_magnitude} M ({rep.calculated_metrics?.max_mag_place})</span>
                      <span>•</span>
                      <span><b>Alertas Tsunami:</b> {rep.calculated_metrics?.tsunami_alerts_count}</span>
                    </div>
                  </div>
                ))}

                {reports.length === 0 && (
                  <div className="p-12 text-center text-slate-500 text-sm bg-slate-900 rounded-xl border border-slate-800">
                    No hay reportes generados aún. Ejecute el DAG 03 en Airflow o el botón de regeneración.
                  </div>
                )}
              </div>
            </div>
          )}

          {/* TAB 3: PIPELINE STATUS */}
          {activeTab === 'pipeline' && (
            <div className="space-y-6">
              <div>
                <h2 className="text-lg font-bold text-white">Salud del Pipeline y Métricas Lakehouse</h2>
                <p className="text-xs text-slate-400">Monitoreo de tareas Airflow, frescura y validaciones deterministas.</p>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                <div className="p-4 bg-slate-900 border border-slate-800 rounded-xl">
                  <span className="text-xs text-slate-400">Estado General</span>
                  <div className="text-xl font-bold text-emerald-400 mt-1 flex items-center space-x-1.5">
                    <CheckCircle2 className="w-5 h-5" />
                    <span>{pipelineStatus?.status || 'OPERACIONAL'}</span>
                  </div>
                </div>

                <div className="p-4 bg-slate-900 border border-slate-800 rounded-xl">
                  <span className="text-xs text-slate-400">Eventos en Gold</span>
                  <div className="text-xl font-bold text-white mt-1">{pipelineStatus?.total_curated_events || 0}</div>
                  <span className="text-[10px] text-emerald-400">Única tabla en Serving DB</span>
                </div>

                <div className="p-4 bg-slate-900 border border-slate-800 rounded-xl">
                  <span className="text-xs text-slate-400">Auto-Remediados</span>
                  <div className="text-xl font-bold text-emerald-400 mt-1">{pipelineStatus?.total_auto_healed_events || 0}</div>
                  <span className="text-[10px] text-slate-400">Reparados en caliente</span>
                </div>

                <div className="p-4 bg-slate-900 border border-slate-800 rounded-xl">
                  <span className="text-xs text-slate-400">Rechazados / Cuarentena</span>
                  <div className="text-xl font-bold text-amber-400 mt-1">{pipelineStatus?.total_quarantined_events || 0}</div>
                  <span className="text-[10px] text-slate-400">Irrecuperables</span>
                </div>
              </div>

              {/* Airflow DAGs Reference */}
              <div className="bg-slate-900 border border-slate-800 rounded-xl p-5 space-y-3">
                <h3 className="text-sm font-bold text-white">Topología de DAGs en Apache Airflow</h3>
                <div className="space-y-2 text-xs">
                  <div className="p-3 bg-slate-950 rounded-lg flex items-center justify-between border border-slate-800">
                    <div>
                      <span className="font-mono font-bold text-emerald-400">dag_01_usgs_ingestion_bronze</span>
                      <p className="text-slate-400 text-[11px]">Ingesta inmutable cada 5 min hacia MinIO Bronze (GeoJSON). Soporta backfill 30d.</p>
                    </div>
                    <span className="px-2 py-0.5 bg-emerald-500/20 text-emerald-400 rounded text-[10px]">Activo (*/5)</span>
                  </div>
                  <div className="p-3 bg-slate-950 rounded-lg flex items-center justify-between border border-slate-800">
                    <div>
                      <span className="font-mono font-bold text-emerald-400">dag_02_spark_silver_gold</span>
                      <p className="text-slate-400 text-[11px]">Dataset trigger: MinIO Bronze &rarr; MinIO Silver (Parquet) &rarr; PostgreSQL Gold (UPSERT).</p>
                    </div>
                    <span className="px-2 py-0.5 bg-indigo-500/20 text-indigo-400 rounded text-[10px]">Dataset Trigger</span>
                  </div>
                  <div className="p-3 bg-slate-950 rounded-lg flex items-center justify-between border border-slate-800">
                    <div>
                      <span className="font-mono font-bold text-emerald-400">dag_03_ai_reports_and_rag</span>
                      <p className="text-slate-400 text-[11px]">DBSCAN anomalías, informe ejecutivo LLM y vectorización pgvector para RAG.</p>
                    </div>
                    <span className="px-2 py-0.5 bg-purple-500/20 text-purple-400 rounded text-[10px]">Daily @ 00:00</span>
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* TAB 4: ADMIN PANEL */}
          {activeTab === 'admin' && session.role === 'admin' && (
            <div className="space-y-6">
              <div>
                <h2 className="text-lg font-bold text-white">Panel de Administración y Control</h2>
                <p className="text-xs text-slate-400">Acciones privilegiadas y auditoría de cuarentena.</p>
              </div>

              {/* Action Buttons */}
              <div className="p-5 bg-slate-900 border border-slate-800 rounded-xl space-y-3">
                <h3 className="text-sm font-bold text-white">Disparo Manual de Cargas Históricas (Backfill)</h3>
                <p className="text-xs text-slate-400">
                  Descarga y procesa hasta 30 días de eventos históricos de USGS sin duplicar registros existentes.
                </p>
                <button
                  onClick={handleTriggerBackfill}
                  disabled={loading}
                  className="px-4 py-2.5 bg-purple-600 hover:bg-purple-500 text-white rounded-lg text-xs font-semibold transition flex items-center space-x-2"
                >
                  <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
                  <span>Lanzar Backfill de 30 días</span>
                </button>
              </div>

              {/* Quarantined Records Table */}
              <div className="bg-slate-900 border border-slate-800 rounded-xl p-4">
                <h3 className="text-sm font-bold text-white mb-2">Auditoría de Cuarentena (Registros Rechazados)</h3>
                <p className="text-xs text-slate-400 mb-4">
                  Eventos con fallos estructurales irrecuperables (coordenadas fuera de planeta o ID ausente).
                </p>

                <div className="overflow-x-auto">
                  <table className="w-full text-left text-xs">
                    <thead className="bg-slate-950 text-slate-400 uppercase tracking-wider text-[10px]">
                      <tr>
                        <th className="p-3">ID Rechazo</th>
                        <th className="p-3">Raw Event ID</th>
                        <th className="p-3">Regla Violada</th>
                        <th className="p-3">Motivo del Rechazo</th>
                        <th className="p-3">Fecha</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-800/60 text-slate-300 font-mono text-[11px]">
                      {quarantineItems.map((q: any) => (
                        <tr key={q.quarantine_id}>
                          <td className="p-3 text-slate-400">{q.quarantine_id.slice(0, 8)}...</td>
                          <td className="p-3 text-amber-400">{q.raw_event_id || 'N/A'}</td>
                          <td className="p-3">{q.rule_violated}</td>
                          <td className="p-3 text-red-400">{q.rejection_reason}</td>
                          <td className="p-3 text-slate-500">{new Date(q.rejected_at).toLocaleString()}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {quarantineItems.length === 0 && (
                    <div className="p-6 text-center text-emerald-400 text-xs">
                      Excelente: 0 registros en cuarentena. La auto-remediación en vuelo ha curado el 100% de los datos.
                    </div>
                  )}
                </div>
              </div>
            </div>
          )}
        </main>
      </div>

      {/* RAG AI CHATBOT DRAWER */}
      {chatOpen && (
        <div className="fixed bottom-4 right-4 w-96 h-[520px] bg-slate-900 border border-slate-700/80 rounded-2xl shadow-2xl flex flex-col z-50 overflow-hidden">
          {/* Header */}
          <div className="p-3.5 bg-slate-800/80 border-b border-slate-700 flex items-center justify-between">
            <div className="flex items-center space-x-2">
              <Bot className="w-5 h-5 text-indigo-400" />
              <div>
                <h4 className="text-xs font-bold text-white">SismoBot • RAG Especialista</h4>
                <span className="text-[10px] text-emerald-400">pgvector HNSW (768d)</span>
              </div>
            </div>
            <button 
              onClick={() => setChatOpen(false)}
              className="p-1 text-slate-400 hover:text-white rounded"
            >
              <XCircle className="w-4 h-4" />
            </button>
          </div>

          {/* Messages */}
          <div className="flex-1 p-3 overflow-y-auto space-y-3 text-xs">
            {chatMessages.map((m, idx) => (
              <div key={idx} className={`flex flex-col ${m.sender === 'user' ? 'items-end' : 'items-start'}`}>
                <div className={`p-3 rounded-xl max-w-[85%] leading-relaxed ${m.sender === 'user' ? 'bg-emerald-600 text-white rounded-br-none' : 'bg-slate-800 text-slate-200 rounded-bl-none border border-slate-700/60'}`}>
                  {m.text}
                </div>
                {m.sources && m.sources.length > 0 && (
                  <div className="mt-1 space-y-0.5 text-[10px] text-slate-500">
                    <span>Fuentes citadas:</span>
                    {m.sources.map((s, i) => (
                      <span key={i} className="inline-block ml-1 px-1.5 py-0.2 bg-slate-800 rounded font-mono text-indigo-400">
                        {s.ref_id} (sim: {s.similarity})
                      </span>
                    ))}
                  </div>
                )}
              </div>
            ))}
            {chatLoading && (
              <div className="flex items-center space-x-2 text-slate-400 text-xs italic">
                <RefreshCw className="w-3.5 h-3.5 animate-spin" />
                <span>Consultando pgvector y generando respuesta...</span>
              </div>
            )}
          </div>

          {/* Input form */}
          <form onSubmit={handleSendMessage} className="p-3 bg-slate-800/50 border-t border-slate-800 flex items-center space-x-2">
            <input
              type="text"
              value={inputMessage}
              onChange={e => setInputMessage(e.target.value)}
              placeholder="Pregunta sobre sismos o el pipeline..."
              className="flex-1 px-3 py-2 bg-slate-900 border border-slate-700 rounded-lg text-xs text-white focus:outline-none focus:border-indigo-500"
            />
            <button
              type="submit"
              disabled={chatLoading}
              className="p-2 bg-indigo-600 hover:bg-indigo-500 text-white rounded-lg transition"
            >
              <Send className="w-3.5 h-3.5" />
            </button>
          </form>
        </div>
      )}
    </div>
  );
}
