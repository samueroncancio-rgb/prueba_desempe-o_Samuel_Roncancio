import React, { useState, useEffect, useRef } from 'react';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';
import Chart from 'chart.js/auto';
import { 
  Activity, Shield, AlertTriangle, Database, Bot, RefreshCw, 
  Download, LogOut, CheckCircle2, XCircle, Search, Filter, 
  MapPin, Send, Server, Sparkles, User, Info, ArrowRight, Globe, Navigation,
  BarChart2, PieChart, TrendingUp, Flame, ShieldAlert, Layers, Calendar, FileText, ChevronDown,
  Zap, Clock, Target, SunMedium
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
  const markersGroupRef = useRef<any>(null);

  // Analytics & Multi-timeframe state for Reports
  const [reportTimeframe, setReportTimeframe] = useState<'daily' | 'weekly' | 'monthly' | 'yearly'>('daily');
  const [analyticsData, setAnalyticsData] = useState<any>(null);
  const [timeseriesData, setTimeseriesData] = useState<any[]>([]);
  const [selectedReportIndex, setSelectedReportIndex] = useState<number>(0);

  // Chart canvas refs & instance refs (8 Charts Suite)
  const magChartRef = useRef<HTMLCanvasElement | null>(null);
  const depthChartRef = useRef<HTMLCanvasElement | null>(null);
  const trendChartRef = useRef<HTMLCanvasElement | null>(null);
  const energyChartRef = useRef<HTMLCanvasElement | null>(null);
  const regionChartRef = useRef<HTMLCanvasElement | null>(null);
  const scatterChartRef = useRef<HTMLCanvasElement | null>(null);
  const diurnalChartRef = useRef<HTMLCanvasElement | null>(null);
  const significanceChartRef = useRef<HTMLCanvasElement | null>(null);

  const magChartInstance = useRef<any>(null);
  const depthChartInstance = useRef<any>(null);
  const trendChartInstance = useRef<any>(null);
  const energyChartInstance = useRef<any>(null);
  const regionChartInstance = useRef<any>(null);
  const scatterChartInstance = useRef<any>(null);
  const diurnalChartInstance = useRef<any>(null);
  const significanceChartInstance = useRef<any>(null);

  const focusOnEvent = (ev: any) => {
    if (mapRef.current && ev.latitude != null && ev.longitude != null) {
      mapRef.current.flyTo([ev.latitude, ev.longitude], 6, { duration: 1.5 });
      document.getElementById('map-world-section')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
  };

  const resetWorldView = () => {
    if (mapRef.current) {
      mapRef.current.flyTo([20, 0], 2, { duration: 1.2 });
    }
  };

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

      // Multi-timeframe analytics for Reports Suite
      const anRes = await fetch(`${API_BASE}/api/reports/analytics?timeframe=${reportTimeframe}`, { headers });
      if (anRes.ok) setAnalyticsData(await anRes.json());

      // Timeseries Analytics for Reports Dashboard
      const tsRes = await fetch(`${API_BASE}/api/stats/timeseries?days=7`, { headers });
      if (tsRes.ok) setTimeseriesData(await tsRes.json());

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

  // Reactive reload when timeframe changes
  useEffect(() => {
    if (!session) return;
    const fetchTimeframeAnalytics = async () => {
      try {
        const res = await fetch(`${API_BASE}/api/reports/analytics?timeframe=${reportTimeframe}`, {
          headers: { Authorization: `Bearer ${session.token}` }
        });
        if (res.ok) {
          const data = await res.json();
          setAnalyticsData(data);
        }
      } catch (e) {}
    };
    fetchTimeframeAnalytics();
  }, [session, reportTimeframe]);

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

  // Helper to render bright red location markers on the world map
  const renderMarkers = (map: any, group: any, eventList: any[]) => {
    if (!group) return;
    group.clearLayers();
    if (!eventList || eventList.length === 0) return;

    eventList.forEach((ev: any) => {
      const lat = parseFloat(ev.latitude);
      const lon = parseFloat(ev.longitude);
      if (isNaN(lat) || isNaN(lon)) return;

      // Custom high-precision Red Location Pin with animated radar ping
      const redPinIcon = L.divIcon({
        className: 'sismowatch-red-marker',
        html: `
          <div style="position: relative; display: flex; align-items: center; justify-content: center; width: 34px; height: 34px; cursor: pointer;">
            <span class="sismo-red-pulse" style="position: absolute; width: 28px; height: 28px; border-radius: 50%; background-color: rgba(239, 68, 68, 0.5);"></span>
            <span style="position: absolute; width: 18px; height: 18px; border-radius: 50%; background-color: rgba(220, 38, 38, 0.75);"></span>
            <div style="position: relative; width: 12px; height: 12px; border-radius: 50%; background-color: #dc2626; border: 2.5px solid #ffffff; box-shadow: 0 0 10px rgba(220, 38, 38, 1);"></div>
          </div>
        `,
        iconSize: [34, 34],
        iconAnchor: [17, 17],
        popupAnchor: [0, -16]
      });

      const marker = L.marker([lat, lon], { icon: redPinIcon });

      // Energy Shockwave Halo in transparent red
      const shockwave = L.circleMarker([lat, lon], {
        radius: Math.max(12, (parseFloat(ev.mag) || 1) * 4.5),
        color: '#dc2626',
        weight: 1.5,
        opacity: 0.85,
        fillColor: '#ef4444',
        fillOpacity: 0.2
      });

      const popupContent = `
        <div style="font-family: system-ui, -apple-system, sans-serif; color: #0f172a; padding: 4px; min-width: 220px;">
          <div style="display: flex; align-items: center; gap: 6px; margin-bottom: 6px; border-bottom: 2px solid #dc2626; padding-bottom: 4px;">
            <span style="display: inline-block; width: 10px; height: 10px; border-radius: 50%; background-color: #dc2626; box-shadow: 0 0 6px #dc2626;"></span>
            <strong style="font-size: 13px; color: #b91c1c; text-transform: uppercase; letter-spacing: 0.5px;">Epicentro Sísmico</strong>
          </div>
          <div style="font-size: 13px; font-weight: 700; color: #0f172a; margin-bottom: 6px; line-height: 1.4;">
            ${ev.place_sanitized || 'Ubicación'}
          </div>
          <div style="font-size: 11px; line-height: 1.7; color: #334155;">
            <div><b>Coordenadas:</b> <span style="font-family: monospace; color: #0369a1; font-weight: 600;">${lat.toFixed(4)}°, ${lon.toFixed(4)}°</span></div>
            <div><b>Magnitud:</b> <span style="color: #dc2626; font-weight: 800; font-size: 13px;">${ev.mag} ${ev.mag_type || 'M'}</span></div>
            <div><b>Profundidad:</b> ${ev.depth_km} km</div>
            <div><b>Alerta Tsunami:</b> ${ev.tsunami_alert ? '<span style="color: #dc2626; font-weight: bold;">⚠️ SÍ</span>' : 'No'}</div>
            <div><b>Hora UTC:</b> ${new Date(ev.event_time).toUTCString()}</div>
            <div><b>Estado:</b> <span style="color: #047857; font-weight: 600;">${ev.is_auto_healed ? 'Auto-Curado en vuelo' : 'Limpio en Gold'}</span></div>
          </div>
        </div>
      `;

      marker.bindPopup(popupContent);
      shockwave.bindPopup(popupContent);

      group.addLayer(shockwave);
      group.addLayer(marker);
    });
  };

  // Initialize Leaflet World Map (Planeta Tierra y Continentes)
  useEffect(() => {
    if (activeTab !== 'dashboard') return;

    const mapElement = document.getElementById('map-container');
    if (!mapElement) return;

    // Reset previous instance cleanly to prevent 'already initialized' crashes
    if (mapRef.current) {
      try {
        mapRef.current.remove();
      } catch (e) {}
      mapRef.current = null;
    }

    if ((mapElement as any)._leaflet_id) {
      delete (mapElement as any)._leaflet_id;
    }

    // World Map centered on Earth showing all continents
    const map = L.map(mapElement, {
      center: [15, 0],
      zoom: 2,
      minZoom: 1.5,
      maxZoom: 18,
      worldCopyJump: true,
      zoomControl: true
    });

    // 1. Real Satellite World Imagery of Earth from Space
    const satelliteLayer = L.tileLayer(
      'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
      {
        attribution: 'Tiles &copy; Esri, Earthstar Geographics',
        maxZoom: 19
      }
    );

    // 2. Topographic World Map
    const topoLayer = L.tileLayer(
      'https://server.arcgisonline.com/ArcGIS/rest/services/World_Topo_Map/MapServer/tile/{z}/{y}/{x}',
      {
        attribution: 'Tiles &copy; Esri &mdash; National Geographic',
        maxZoom: 19
      }
    );

    // Add satelliteLayer as primary default map
    satelliteLayer.addTo(map);

    // Add interactive layer selector in top-right
    L.control.layers(
      {
        "🛰️ Satélite Planeta Tierra": satelliteLayer,
        "⛰️ Relieve y Topografía": topoLayer
      },
      null,
      { position: 'topright' }
    ).addTo(map);

    const markersGroup = L.layerGroup().addTo(map);
    markersGroupRef.current = markersGroup;
    mapRef.current = map;

    // Render red earthquake points
    renderMarkers(map, markersGroup, events);

    // Smooth resize trigger
    const timer = setTimeout(() => {
      map.invalidateSize();
    }, 200);

    return () => {
      clearTimeout(timer);
      if (mapRef.current) {
        try {
          mapRef.current.remove();
        } catch (e) {}
        mapRef.current = null;
      }
    };
  }, [activeTab]);

  // Update red markers when events update
  useEffect(() => {
    if (mapRef.current && markersGroupRef.current) {
      renderMarkers(mapRef.current, markersGroupRef.current, events);
    }
  }, [events]);

  // Analytical Charts Engine for Reports View (8-Chart Suite)
  useEffect(() => {
    if (activeTab !== 'reports') return;

    // Clean up prior 8 Chart.js instances
    [
      magChartInstance, depthChartInstance, trendChartInstance, energyChartInstance,
      regionChartInstance, scatterChartInstance, diurnalChartInstance, significanceChartInstance
    ].forEach(inst => {
      if (inst.current) {
        inst.current.destroy();
        inst.current = null;
      }
    });

    // Global Chart.js styling
    Chart.defaults.color = '#94a3b8';
    Chart.defaults.font.family = "'Inter', system-ui, sans-serif";

    // Data extraction helpers
    const magData = analyticsData?.magnitude_distribution || {
      micro: events.filter(e => parseFloat(e.mag) < 2.0).length,
      menor: events.filter(e => parseFloat(e.mag) >= 2.0 && parseFloat(e.mag) < 4.0).length,
      moderado: events.filter(e => parseFloat(e.mag) >= 4.0 && parseFloat(e.mag) < 5.5).length,
      fuerte: events.filter(e => parseFloat(e.mag) >= 5.5 && parseFloat(e.mag) < 7.0).length,
      mayor: events.filter(e => parseFloat(e.mag) >= 7.0).length
    };

    const depthData = analyticsData?.depth_distribution || {
      shallow: events.filter(e => parseFloat(e.depth_km) <= 30).length,
      intermediate: events.filter(e => parseFloat(e.depth_km) > 30 && parseFloat(e.depth_km) <= 100).length,
      deep: events.filter(e => parseFloat(e.depth_km) > 100).length
    };

    const tsList = (analyticsData?.timeseries && analyticsData.timeseries.length > 0)
      ? analyticsData.timeseries
      : timeseriesData;

    // 1. Chart 1: Magnitude Range Distribution (Bar Chart)
    if (magChartRef.current) {
      magChartInstance.current = new Chart(magChartRef.current, {
        type: 'bar',
        data: {
          labels: ['Micro (<2.0)', 'Menor (2.0-3.9)', 'Moderado (4.0-5.4)', 'Fuerte (5.5-6.9)', 'Mayor (≥7.0)'],
          datasets: [{
            label: 'Cantidad de Sismos',
            data: [magData.micro, magData.menor, magData.moderado, magData.fuerte, magData.mayor],
            backgroundColor: [
              'rgba(16, 185, 129, 0.75)',
              'rgba(59, 130, 246, 0.75)',
              'rgba(245, 158, 11, 0.75)',
              'rgba(239, 68, 68, 0.85)',
              'rgba(168, 85, 247, 0.85)'
            ],
            borderColor: ['#10b981', '#3b82f6', '#f59e0b', '#ef4444', '#a855f7'],
            borderWidth: 1.5,
            borderRadius: 6
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { display: false },
            tooltip: { backgroundColor: '#0f172a', borderColor: '#334155', borderWidth: 1, padding: 10 }
          },
          scales: {
            x: { grid: { display: false }, ticks: { color: '#94a3b8', font: { size: 10 } } },
            y: { grid: { color: 'rgba(51, 65, 85, 0.3)' }, ticks: { color: '#94a3b8', stepSize: 1, precision: 0 } }
          }
        }
      });
    }

    // 2. Chart 2: Tectonic Depth Classification (Doughnut Chart)
    if (depthChartRef.current) {
      depthChartInstance.current = new Chart(depthChartRef.current, {
        type: 'doughnut',
        data: {
          labels: ['Superficial (≤30km)', 'Intermedio (30-100km)', 'Profundo (>100km)'],
          datasets: [{
            data: [
              depthData.shallow || (events.length === 0 ? 1 : 0),
              depthData.intermediate,
              depthData.deep
            ],
            backgroundColor: ['rgba(239, 68, 68, 0.8)', 'rgba(245, 158, 11, 0.8)', 'rgba(99, 102, 241, 0.85)'],
            borderColor: '#0f172a',
            borderWidth: 2
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          cutout: '65%',
          plugins: {
            legend: { position: 'bottom', labels: { color: '#cbd5e1', font: { size: 10 }, padding: 8 } }
          }
        }
      });
    }

    // 3. Chart 3: Time Series Activity & Magnitude Trend (Dual Axis)
    const timeLabels = tsList.length > 0
      ? tsList.map((t: any) => {
          const d = new Date(t.time_bucket);
          return reportTimeframe === 'daily'
            ? d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
            : d.toLocaleDateString([], { month: 'short', day: 'numeric' });
        })
      : events.slice(0, 8).reverse().map(e => new Date(e.event_time).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }));

    const timeCounts = tsList.length > 0
      ? tsList.map((t: any) => t.event_count)
      : events.slice(0, 8).reverse().map((_, idx) => idx + 1);

    const timeMags = tsList.length > 0
      ? tsList.map((t: any) => t.avg_magnitude)
      : events.slice(0, 8).reverse().map(e => parseFloat(e.mag) || 1);

    if (trendChartRef.current) {
      trendChartInstance.current = new Chart(trendChartRef.current, {
        type: 'line',
        data: {
          labels: timeLabels.length > 0 ? timeLabels : ['00:00', '06:00', '12:00', '18:00', '24:00'],
          datasets: [
            {
              label: 'Eventos Registrados',
              data: timeCounts.length > 0 ? timeCounts : [1, 2, 4, 3, 5],
              borderColor: '#10b981',
              backgroundColor: 'rgba(16, 185, 129, 0.15)',
              fill: true,
              tension: 0.35,
              yAxisID: 'y'
            },
            {
              label: 'Magnitud Promedio',
              data: timeMags.length > 0 ? timeMags : [0.9, 1.2, 1.4, 0.8, 1.3],
              borderColor: '#f59e0b',
              backgroundColor: 'transparent',
              borderDash: [4, 4],
              tension: 0.3,
              yAxisID: 'y1'
            }
          ]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { labels: { color: '#cbd5e1', font: { size: 10 } } } },
          scales: {
            x: { grid: { color: 'rgba(51, 65, 85, 0.2)' }, ticks: { color: '#94a3b8', font: { size: 10 } } },
            y: { type: 'linear', position: 'left', grid: { color: 'rgba(51, 65, 85, 0.3)' }, ticks: { color: '#10b981', precision: 0 } },
            y1: { type: 'linear', position: 'right', grid: { drawOnChartArea: false }, ticks: { color: '#f59e0b', precision: 1 } }
          }
        }
      });
    }

    // 4. Chart 4: Cumulative Seismic Energy Release (Gutenberg-Richter Formula - Megajoules)
    const energyVals = tsList.length > 0
      ? tsList.map((t: any) => t.energy_mj || 0)
      : events.slice(0, 8).map(e => Math.pow(10, 4.8 + 1.5 * (parseFloat(e.mag) || 1)) / 1e6);

    if (energyChartRef.current) {
      energyChartInstance.current = new Chart(energyChartRef.current, {
        type: 'line',
        data: {
          labels: timeLabels.length > 0 ? timeLabels : ['00:00', '06:00', '12:00', '18:00', '24:00'],
          datasets: [{
            label: 'Energía Sísmica (Megajoules)',
            data: energyVals.length > 0 ? energyVals : [100, 250, 400, 1200, 5000],
            borderColor: '#f43f5e',
            backgroundColor: 'rgba(244, 63, 94, 0.2)',
            fill: true,
            tension: 0.35,
            borderWidth: 2,
            pointBackgroundColor: '#f43f5e',
            pointRadius: 4
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { display: false },
            tooltip: {
              callbacks: {
                label: (ctx: any) => ` Energía: ${Number(ctx.raw).toLocaleString()} MJ`
              }
            }
          },
          scales: {
            x: { grid: { color: 'rgba(51, 65, 85, 0.2)' }, ticks: { color: '#94a3b8', font: { size: 10 } } },
            y: { grid: { color: 'rgba(51, 65, 85, 0.3)' }, ticks: { color: '#f43f5e', font: { size: 10 } } }
          }
        }
      });
    }

    // 5. Chart 5: Top 5 Active Seismic Zones (Horizontal Bar Chart)
    const topRegs = analyticsData?.top_regions || [];
    const regLabels = topRegs.length > 0
      ? topRegs.map((r: any) => r.region)
      : ['California', 'Alaska', 'Nevada', 'Hawaii', 'Chile'];
    const regValues = topRegs.length > 0
      ? topRegs.map((r: any) => r.count)
      : [4, 3, 2, 1, 1];

    if (regionChartRef.current) {
      regionChartInstance.current = new Chart(regionChartRef.current, {
        type: 'bar',
        data: {
          labels: regLabels,
          datasets: [{
            label: 'Eventos por Región',
            data: regValues,
            backgroundColor: 'rgba(56, 189, 248, 0.75)',
            borderColor: '#38bdf8',
            borderWidth: 1.5,
            borderRadius: 6
          }]
        },
        options: {
          indexAxis: 'y',
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { display: false } },
          scales: {
            x: { grid: { color: 'rgba(51, 65, 85, 0.3)' }, ticks: { color: '#94a3b8', precision: 0 } },
            y: { grid: { display: false }, ticks: { color: '#cbd5e1', font: { size: 10 } } }
          }
        }
      });
    }

    // 6. Chart 6: Hypocentral Depth vs. Magnitude Correlation (Scatter Plot)
    const scatterData = (analyticsData?.scatter_points && analyticsData.scatter_points.length > 0)
      ? analyticsData.scatter_points.map((p: any) => ({ x: p.x, y: p.y, place: p.place }))
      : events.map(e => ({ x: parseFloat(e.mag) || 1, y: parseFloat(e.depth_km) || 10, place: e.place_sanitized }));

    if (scatterChartRef.current) {
      scatterChartInstance.current = new Chart(scatterChartRef.current, {
        type: 'scatter',
        data: {
          datasets: [{
            label: 'Hipocentros',
            data: scatterData,
            backgroundColor: 'rgba(239, 68, 68, 0.75)',
            borderColor: '#ffffff',
            borderWidth: 1,
            pointRadius: 6,
            pointHoverRadius: 9
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { display: false },
            tooltip: {
              callbacks: {
                label: (ctx: any) => {
                  const raw = ctx.raw as any;
                  return ` Mag: ${raw.x} M | Prof: ${raw.y} km (${raw.place || ''})`;
                }
              }
            }
          },
          scales: {
            x: {
              title: { display: true, text: 'Magnitud (M)', color: '#94a3b8', font: { size: 10 } },
              grid: { color: 'rgba(51, 65, 85, 0.2)' },
              ticks: { color: '#94a3b8' }
            },
            y: {
              reverse: true, // Seismological convention: depth increases downward into Earth's crust!
              title: { display: true, text: 'Profundidad (km)', color: '#94a3b8', font: { size: 10 } },
              grid: { color: 'rgba(51, 65, 85, 0.3)' },
              ticks: { color: '#94a3b8' }
            }
          }
        }
      });
    }

    // 7. Chart 7: Diurnal Seismic Activity Cycle (00h - 23h UTC)
    const diurnalVals = analyticsData?.hourly_distribution || Array(24).fill(0);
    const diurnalLabels = Array.from({ length: 24 }, (_, i) => `${String(i).padStart(2, '0')}h`);

    if (diurnalChartRef.current) {
      diurnalChartInstance.current = new Chart(diurnalChartRef.current, {
        type: 'bar',
        data: {
          labels: diurnalLabels,
          datasets: [{
            label: 'Eventos por Franja Horaria UTC',
            data: diurnalVals,
            backgroundColor: 'rgba(168, 85, 247, 0.75)',
            borderColor: '#a855f7',
            borderWidth: 1,
            borderRadius: 4
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { display: false } },
          scales: {
            x: { grid: { display: false }, ticks: { color: '#94a3b8', font: { size: 9 } } },
            y: { grid: { color: 'rgba(51, 65, 85, 0.3)' }, ticks: { color: '#94a3b8', precision: 0 } }
          }
        }
      });
    }

    // 8. Chart 8: USGS Significance & Severity Index (Doughnut Chart)
    const sigData = analyticsData?.significance_distribution || {
      low: events.filter(e => (e.significance || 0) < 100).length || 1,
      moderate: events.filter(e => (e.significance || 0) >= 100 && (e.significance || 0) < 300).length,
      high: events.filter(e => (e.significance || 0) >= 300 && (e.significance || 0) < 600).length,
      critical: events.filter(e => (e.significance || 0) >= 600).length
    };

    if (significanceChartRef.current) {
      significanceChartInstance.current = new Chart(significanceChartRef.current, {
        type: 'doughnut',
        data: {
          labels: ['Baja (<100)', 'Moderada (100-299)', 'Alta (300-599)', 'Crítica (≥600)'],
          datasets: [{
            data: [sigData.low, sigData.moderate, sigData.high, sigData.critical],
            backgroundColor: [
              'rgba(16, 185, 129, 0.8)',
              'rgba(59, 130, 246, 0.8)',
              'rgba(245, 158, 11, 0.8)',
              'rgba(239, 68, 68, 0.85)'
            ],
            borderColor: '#0f172a',
            borderWidth: 2
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          cutout: '60%',
          plugins: {
            legend: { position: 'bottom', labels: { color: '#cbd5e1', font: { size: 10 }, padding: 6 } }
          }
        }
      });
    }

    return () => {
      [
        magChartInstance, depthChartInstance, trendChartInstance, energyChartInstance,
        regionChartInstance, scatterChartInstance, diurnalChartInstance, significanceChartInstance
      ].forEach(inst => {
        if (inst.current) {
          inst.current.destroy();
          inst.current = null;
        }
      });
    };
  }, [activeTab, analyticsData, reports, events, timeseriesData, reportTimeframe]);

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

  // Export Curated Telemetry as CSV
  const handleExportCsv = async () => {
    if (!session) return;
    try {
      const res = await fetch(`${API_BASE}/api/reports/export?format=csv`, {
        headers: { Authorization: `Bearer ${session.token}` }
      });
      if (!res.ok) throw new Error('Error al descargar CSV de telemetría.');
      const blob = await res.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `sismowatch_curated_telemetry_${new Date().toISOString().slice(0, 10)}.csv`;
      document.body.appendChild(a);
      a.click();
      a.remove();
      window.URL.revokeObjectURL(url);
    } catch (err: any) {
      alert(err.message || 'Error al exportar telemetría');
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

  const activeReport = reports[selectedReportIndex] || reports[0] || null;
  const reportMetrics = activeReport?.calculated_metrics || {
    total_events: kpis?.total_curated_events || events.length,
    avg_magnitude: kpis?.avg_magnitude_24h || 0,
    max_magnitude: kpis?.max_magnitude_24h || 0,
    max_mag_place: 'N/A',
    deep_events_count: events.filter((e: any) => parseFloat(e.depth_km) > 100).length,
    tsunami_alerts_count: kpis?.tsunami_alerts_24h || 0,
  };

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
            <BarChart2 className="w-4 h-4" />
            <span>Reportes</span>
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
              <span>Panel Admin</span>
            </button>
          )}
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

              {/* World Map Section - Mapa Mundi Completo en Tiempo Real */}
              <div id="map-world-section" className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-2xl space-y-4">
                <div className="flex flex-col md:flex-row md:items-center justify-between gap-3 pb-3 border-b border-slate-800">
                  <div>
                    <div className="flex items-center space-x-2">
                      <Globe className="w-5 h-5 text-red-500 animate-pulse" />
                      <h2 className="text-base font-bold text-white tracking-wide">
                        Mapa Mundi de Monitoreo Sísmico Global en Tiempo Real
                      </h2>
                    </div>
                    <p className="text-xs text-slate-400 mt-0.5">
                      Ubicación exacta con marcadores georreferenciados en color rojo y halos de propagación sísmica
                    </p>
                  </div>

                  <div className="flex items-center space-x-2.5">
                    <span className="px-3 py-1 bg-red-500/10 border border-red-500/30 rounded-full text-xs text-red-400 font-semibold flex items-center space-x-1.5">
                      <span className="w-2 h-2 rounded-full bg-red-500 animate-ping"></span>
                      <span>{events.length} sismos proyectados</span>
                    </span>

                    <button
                      onClick={resetWorldView}
                      className="px-3 py-1.5 bg-slate-800 hover:bg-slate-700 text-slate-200 border border-slate-700 rounded-lg text-xs font-medium transition flex items-center space-x-1.5 shadow-sm"
                      title="Restablecer zoom y centrar en el mapa del mundo completo"
                    >
                      <Navigation className="w-3.5 h-3.5 text-sky-400" />
                      <span>Vista Mundial Completa</span>
                    </button>
                  </div>
                </div>

                {/* Full-Width World Map Container */}
                <div className="relative rounded-xl overflow-hidden border border-slate-800 bg-slate-950">
                  <div id="map-container" className="h-[520px] w-full z-0" />

                  {/* On-Map Legend Overlay */}
                  <div className="absolute bottom-4 right-4 z-[400] bg-slate-900/90 backdrop-blur-md border border-slate-700/80 rounded-xl p-3 shadow-2xl text-[11px] space-y-1.5 max-w-xs pointer-events-auto">
                    <div className="font-semibold text-slate-200 flex items-center space-x-1.5 border-b border-slate-800 pb-1">
                      <MapPin className="w-3.5 h-3.5 text-red-400" />
                      <span>Simbología Sísmica</span>
                    </div>
                    <div className="flex items-center space-x-2 text-slate-300">
                      <span className="w-3 h-3 rounded-full bg-red-600 border border-white shadow"></span>
                      <span>Punto rojo: Epicentro exacto</span>
                    </div>
                    <div className="flex items-center space-x-2 text-slate-400">
                      <span className="w-3 h-3 rounded-full border border-red-500 bg-red-500/20"></span>
                      <span>Halo rojo: Radio de energía</span>
                    </div>
                    <div className="text-[10px] text-slate-400 italic pt-0.5">
                      * Haz clic en cualquier punto rojo o en la tabla para ubicar.
                    </div>
                  </div>
                </div>
              </div>

              {/* Filters & Export Bar */}
              <div className="bg-slate-900 border border-slate-800 rounded-xl p-4 grid grid-cols-1 md:grid-cols-3 gap-4 items-center">
                <div>
                  <div className="flex justify-between items-center mb-1">
                    <label className="text-xs text-slate-400 font-medium">Magnitud Mínima</label>
                    <span className="text-xs text-red-400 font-bold font-mono">{minMag} M</span>
                  </div>
                  <input 
                    type="range" 
                    min="0" 
                    max="8" 
                    step="0.5" 
                    value={minMag}
                    onChange={e => setMinMag(parseFloat(e.target.value))}
                    className="w-full accent-red-500 cursor-pointer h-1.5 bg-slate-800 rounded-lg"
                  />
                </div>

                <div>
                  <label className="text-xs text-slate-400 font-medium block mb-1">Buscar por Región o País</label>
                  <div className="relative">
                    <Search className="w-3.5 h-3.5 absolute left-3 top-2.5 text-slate-500" />
                    <input 
                      type="text" 
                      placeholder="Ej. California, Chile, Alaska, Japón..." 
                      value={regionFilter}
                      onChange={e => setRegionFilter(e.target.value)}
                      className="w-full pl-9 pr-3 py-1.5 bg-slate-950 border border-slate-800 rounded-lg text-xs text-white focus:outline-none focus:border-red-500 transition"
                    />
                  </div>
                </div>

                <div className="flex items-end">
                  <button
                    onClick={handleExportCsv}
                    className="w-full py-2 bg-slate-800 hover:bg-slate-700 text-slate-200 rounded-lg text-xs font-semibold flex items-center justify-center space-x-2 transition border border-slate-700 shadow-sm"
                  >
                    <Download className="w-3.5 h-3.5 text-emerald-400" />
                    <span>Exportar Telemetría Curada (CSV)</span>
                  </button>
                </div>
              </div>

              {/* Curated Events Table */}
              <div className="bg-slate-900 border border-slate-800 rounded-xl p-4">
                <div className="flex items-center justify-between mb-4">
                  <div>
                    <h3 className="text-sm font-bold text-white">Hechos Limpios en GOLD (fact_earthquake_events)</h3>
                    <p className="text-xs text-slate-400">Haz clic en cualquier fila para ubicar su epicentro en el mapa</p>
                  </div>
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
                        <th className="p-3 text-right">Acción</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-800/60 text-slate-300">
                      {events.map((ev: any) => (
                        <tr 
                          key={ev.event_id} 
                          onClick={() => focusOnEvent(ev)}
                          className="hover:bg-slate-800/60 transition cursor-pointer"
                        >
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
                          <td className="p-3 text-right">
                            <button
                              onClick={(e) => {
                                e.stopPropagation();
                                focusOnEvent(ev);
                              }}
                              className="px-2.5 py-1 bg-red-600/20 hover:bg-red-600/40 text-red-400 border border-red-500/30 rounded text-[11px] font-medium transition inline-flex items-center space-x-1"
                              title="Centrar mapa en este epicentro"
                            >
                              <MapPin className="w-3 h-3 text-red-400" />
                              <span>Ubicar</span>
                            </button>
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

          {/* TAB 2: REPORTS & MULTI-TIMEFRAME ANALYTICS DASHBOARD */}
          {activeTab === 'reports' && (
            <div className="space-y-6">
              {/* Executive Header & Controls */}
              <div className="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-4 bg-slate-900 border border-slate-800 p-5 rounded-2xl shadow-lg">
                <div className="space-y-1">
                  <div className="flex items-center space-x-2">
                    <span className="p-1.5 bg-emerald-500/10 border border-emerald-500/30 rounded-lg text-emerald-400">
                      <BarChart2 className="w-5 h-5" />
                    </span>
                    <h2 className="text-xl font-bold text-white tracking-tight">Centro Ejecutivo de Reportes e Inteligencia Sísmica</h2>
                    <span className="px-2 py-0.5 bg-emerald-500/20 text-emerald-400 border border-emerald-500/30 rounded-full text-[10px] font-mono font-semibold">
                      MULTI-TIMEFRAME
                    </span>
                  </div>
                  <p className="text-xs text-slate-400 max-w-2xl leading-relaxed">
                    Evaluación cuantitativa de la sismicidad global, perfiles hipocentrales, dispersión geológica, ciclos diurnos y liberación de energía acumulada.
                  </p>
                </div>

                <div className="flex flex-wrap items-center gap-2.5">
                  {/* Timeframe Selector Toggle */}
                  <div className="flex items-center bg-slate-950 p-1 rounded-xl border border-slate-800 gap-1">
                    <button
                      onClick={() => setReportTimeframe('daily')}
                      className={`px-3 py-1.5 rounded-lg text-xs font-semibold flex items-center space-x-1.5 transition ${
                        reportTimeframe === 'daily'
                          ? 'bg-emerald-600 text-white shadow-md'
                          : 'text-slate-400 hover:text-white hover:bg-slate-900'
                      }`}
                    >
                      <Clock className="w-3.5 h-3.5" />
                      <span>Diario (24h)</span>
                    </button>
                    <button
                      onClick={() => setReportTimeframe('weekly')}
                      className={`px-3 py-1.5 rounded-lg text-xs font-semibold flex items-center space-x-1.5 transition ${
                        reportTimeframe === 'weekly'
                          ? 'bg-emerald-600 text-white shadow-md'
                          : 'text-slate-400 hover:text-white hover:bg-slate-900'
                      }`}
                    >
                      <Calendar className="w-3.5 h-3.5" />
                      <span>Semanal (7D)</span>
                    </button>
                    <button
                      onClick={() => setReportTimeframe('monthly')}
                      className={`px-3 py-1.5 rounded-lg text-xs font-semibold flex items-center space-x-1.5 transition ${
                        reportTimeframe === 'monthly'
                          ? 'bg-emerald-600 text-white shadow-md'
                          : 'text-slate-400 hover:text-white hover:bg-slate-900'
                      }`}
                    >
                      <Layers className="w-3.5 h-3.5" />
                      <span>Mensual (30D)</span>
                    </button>
                    <button
                      onClick={() => setReportTimeframe('yearly')}
                      className={`px-3 py-1.5 rounded-lg text-xs font-semibold flex items-center space-x-1.5 transition ${
                        reportTimeframe === 'yearly'
                          ? 'bg-emerald-600 text-white shadow-md'
                          : 'text-slate-400 hover:text-white hover:bg-slate-900'
                      }`}
                    >
                      <TrendingUp className="w-3.5 h-3.5" />
                      <span>Anual (365D)</span>
                    </button>
                  </div>

                  {/* Selector of Historical Reports (Only when in Daily timeframe) */}
                  {reportTimeframe === 'daily' && reports.length > 0 && (
                    <div className="relative">
                      <select
                        value={selectedReportIndex}
                        onChange={(e) => setSelectedReportIndex(Number(e.target.value))}
                        className="bg-slate-950 border border-slate-700 text-slate-200 text-xs rounded-lg px-3 py-2 pr-8 appearance-none focus:outline-none focus:border-emerald-500 cursor-pointer font-mono shadow-sm"
                      >
                        {reports.map((rep: any, idx: number) => (
                          <option key={rep.report_id || idx} value={idx}>
                            📅 {rep.report_date} - {rep.title?.slice(0, 24)}...
                          </option>
                        ))}
                      </select>
                      <ChevronDown className="w-3.5 h-3.5 text-slate-400 absolute right-2.5 top-3 pointer-events-none" />
                    </div>
                  )}

                  <button
                    onClick={handleExportCsv}
                    className="px-3 py-2 bg-slate-800 hover:bg-slate-700 text-slate-200 border border-slate-700 rounded-lg text-xs font-semibold flex items-center space-x-1.5 transition shadow-sm"
                    title="Descargar dataset analítico curado"
                  >
                    <Download className="w-3.5 h-3.5 text-emerald-400" />
                    <span>Exportar CSV</span>
                  </button>

                  {session.role === 'admin' && (
                    <button
                      onClick={async () => {
                        setLoading(true);
                        try {
                          await fetch(`${API_BASE}/api/admin/reports/regenerate`, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${session.token}` },
                            body: JSON.stringify({})
                          });
                          await fetchData();
                        } finally {
                          setLoading(false);
                        }
                      }}
                      className="px-3.5 py-2 bg-emerald-600 hover:bg-emerald-500 text-white rounded-lg text-xs font-semibold flex items-center space-x-1.5 transition shadow-sm"
                    >
                      <RefreshCw className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
                      <span>Regenerar Informe</span>
                    </button>
                  )}
                </div>
              </div>

              {/* Tarjetero de Métricas Ejecutivas (6 Cards Deck) */}
              <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3.5">
                {/* Metric 1: Total Events */}
                <div className="bg-slate-900/90 border border-slate-800 p-4 rounded-xl flex flex-col justify-between hover:border-slate-700 transition">
                  <div className="flex items-center justify-between">
                    <span className="text-[11px] font-medium text-slate-400">Total Eventos</span>
                    <span className="p-1.5 bg-emerald-500/10 text-emerald-400 rounded-lg">
                      <Activity className="w-4 h-4" />
                    </span>
                  </div>
                  <div className="my-2">
                    <div className="text-2xl font-black text-white font-mono">
                      {analyticsData?.summary?.total_events ?? reportMetrics.total_events}
                    </div>
                    <span className="text-[10px] text-slate-400">Ventana: {reportTimeframe}</span>
                  </div>
                  <div className="pt-1 border-t border-slate-800/80 flex items-center text-[10px] text-emerald-400 font-medium">
                    <CheckCircle2 className="w-3 h-3 mr-1" />
                    <span>100% Curados en Gold</span>
                  </div>
                </div>

                {/* Metric 2: Max Magnitude */}
                <div className="bg-slate-900/90 border border-slate-800 p-4 rounded-xl flex flex-col justify-between hover:border-slate-700 transition">
                  <div className="flex items-center justify-between">
                    <span className="text-[11px] font-medium text-slate-400">Pico Sísmico</span>
                    <span className="p-1.5 bg-red-500/10 text-red-400 rounded-lg">
                      <Flame className="w-4 h-4" />
                    </span>
                  </div>
                  <div className="my-2">
                    <div className="text-2xl font-black text-red-400 font-mono">
                      {analyticsData?.summary?.max_magnitude != null
                        ? Number(analyticsData.summary.max_magnitude).toFixed(1)
                        : (reportMetrics.max_magnitude ? Number(reportMetrics.max_magnitude).toFixed(1) : '0.0')} <span className="text-xs font-normal text-slate-400">M</span>
                    </div>
                    <span className="text-[10px] text-slate-400 truncate block max-w-full" title={analyticsData?.summary?.max_mag_place || reportMetrics.max_mag_place}>
                      {analyticsData?.summary?.max_mag_place || reportMetrics.max_mag_place || 'Sin eventos'}
                    </span>
                  </div>
                  <div className="pt-1 border-t border-slate-800/80 flex items-center text-[10px] text-red-400 font-medium">
                    <span>Máxima Liberación</span>
                  </div>
                </div>

                {/* Metric 3: Avg Magnitude */}
                <div className="bg-slate-900/90 border border-slate-800 p-4 rounded-xl flex flex-col justify-between hover:border-slate-700 transition">
                  <div className="flex items-center justify-between">
                    <span className="text-[11px] font-medium text-slate-400">Mag. Promedio</span>
                    <span className="p-1.5 bg-amber-500/10 text-amber-400 rounded-lg">
                      <TrendingUp className="w-4 h-4" />
                    </span>
                  </div>
                  <div className="my-2">
                    <div className="text-2xl font-black text-amber-300 font-mono">
                      {analyticsData?.summary?.avg_magnitude != null
                        ? Number(analyticsData.summary.avg_magnitude).toFixed(2)
                        : (reportMetrics.avg_magnitude ? Number(reportMetrics.avg_magnitude).toFixed(2) : '0.00')} <span className="text-xs font-normal text-slate-400">M</span>
                    </div>
                    <span className="text-[10px] text-slate-400">Media Ponderada</span>
                  </div>
                  <div className="pt-1 border-t border-slate-800/80 flex items-center text-[10px] text-amber-400 font-medium">
                    <span>Intensidad Global</span>
                  </div>
                </div>

                {/* Metric 4: Energy Release (Gutenberg-Richter) */}
                <div className="bg-slate-900/90 border border-slate-800 p-4 rounded-xl flex flex-col justify-between hover:border-slate-700 transition">
                  <div className="flex items-center justify-between">
                    <span className="text-[11px] font-medium text-slate-400">Energía Estimada</span>
                    <span className="p-1.5 bg-rose-500/10 text-rose-400 rounded-lg">
                      <Zap className="w-4 h-4" />
                    </span>
                  </div>
                  <div className="my-2">
                    <div className="text-xl font-black text-rose-400 font-mono truncate" title={`${Number(analyticsData?.summary?.total_energy_mj || 0).toLocaleString()} Megajoules`}>
                      {analyticsData?.summary?.total_energy_mj != null
                        ? `${(analyticsData.summary.total_energy_mj / 1000).toFixed(1)}k`
                        : '0'} <span className="text-xs font-normal text-slate-400">MJ</span>
                    </div>
                    <span className="text-[10px] text-slate-400">Gutenberg-Richter</span>
                  </div>
                  <div className="pt-1 border-t border-slate-800/80 flex items-center text-[10px] text-rose-400 font-medium">
                    <span>Esfuerzo Tectónico</span>
                  </div>
                </div>

                {/* Metric 5: Deep Events */}
                <div className="bg-slate-900/90 border border-slate-800 p-4 rounded-xl flex flex-col justify-between hover:border-slate-700 transition">
                  <div className="flex items-center justify-between">
                    <span className="text-[11px] font-medium text-slate-400">Profundos (&gt;100km)</span>
                    <span className="p-1.5 bg-indigo-500/10 text-indigo-400 rounded-lg">
                      <Layers className="w-4 h-4" />
                    </span>
                  </div>
                  <div className="my-2">
                    <div className="text-2xl font-black text-indigo-300 font-mono">
                      {analyticsData?.summary?.deep_events_count ?? reportMetrics.deep_events_count}
                    </div>
                    <span className="text-[10px] text-slate-400">Zona Wadati-Benioff</span>
                  </div>
                  <div className="pt-1 border-t border-slate-800/80 flex items-center text-[10px] text-indigo-400 font-medium">
                    <span>Manto Superior</span>
                  </div>
                </div>

                {/* Metric 6: Tsunami Alerts */}
                <div className="bg-slate-900/90 border border-slate-800 p-4 rounded-xl flex flex-col justify-between hover:border-slate-700 transition">
                  <div className="flex items-center justify-between">
                    <span className="text-[11px] font-medium text-slate-400">Alertas Tsunami</span>
                    <span className="p-1.5 bg-cyan-500/10 text-cyan-400 rounded-lg">
                      <ShieldAlert className="w-4 h-4" />
                    </span>
                  </div>
                  <div className="my-2">
                    <div className="text-2xl font-black text-cyan-300 font-mono">
                      {analyticsData?.summary?.tsunami_alerts_count ?? reportMetrics.tsunami_alerts_count}
                    </div>
                    <span className="text-[10px] text-slate-400">Boyas DART & USGS</span>
                  </div>
                  <div className="pt-1 border-t border-slate-800/80 flex items-center text-[10px] font-medium">
                    {(analyticsData?.summary?.tsunami_alerts_count ?? reportMetrics.tsunami_alerts_count) > 0 ? (
                      <span className="text-red-400 font-bold">⚠️ Alerta Emitida</span>
                    ) : (
                      <span className="text-emerald-400">🟢 Nivel Nominal</span>
                    )}
                  </div>
                </div>
              </div>

              {/* Suite de Gráficas Analíticas (Grid 8 Cards: 4 Filas x 2 Columnas) */}
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-5">
                {/* Chart 1: Magnitude Distribution */}
                <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 flex flex-col justify-between shadow-lg hover:border-slate-700/80 transition">
                  <div className="flex items-center justify-between mb-3 border-b border-slate-800 pb-3">
                    <div className="flex items-center space-x-2.5">
                      <span className="p-1.5 bg-blue-500/10 text-blue-400 rounded-lg">
                        <BarChart2 className="w-4 h-4" />
                      </span>
                      <div>
                        <h3 className="text-sm font-bold text-white">1. Distribución por Escala de Magnitud</h3>
                        <p className="text-[11px] text-slate-400">Frecuencia sísmica categorizada por bandas Richter/Mw</p>
                      </div>
                    </div>
                    <span className="text-[10px] px-2 py-0.5 bg-slate-800 text-slate-300 rounded font-mono">
                      Histograma
                    </span>
                  </div>
                  
                  <div className="h-64 w-full relative">
                    <canvas ref={magChartRef}></canvas>
                  </div>

                  <div className="mt-3 pt-3 border-t border-slate-800 grid grid-cols-5 gap-1.5 text-center text-[10px]">
                    <div className="bg-slate-950/60 p-1.5 rounded border border-slate-800/60">
                      <span className="text-emerald-400 block font-bold">&lt;2.0</span>
                      <span className="text-slate-400">Micro</span>
                    </div>
                    <div className="bg-slate-950/60 p-1.5 rounded border border-slate-800/60">
                      <span className="text-blue-400 block font-bold">2.0-3.9</span>
                      <span className="text-slate-400">Menor</span>
                    </div>
                    <div className="bg-slate-950/60 p-1.5 rounded border border-slate-800/60">
                      <span className="text-amber-400 block font-bold">4.0-5.4</span>
                      <span className="text-slate-400">Mod.</span>
                    </div>
                    <div className="bg-slate-950/60 p-1.5 rounded border border-slate-800/60">
                      <span className="text-red-400 block font-bold">5.5-6.9</span>
                      <span className="text-slate-400">Fuerte</span>
                    </div>
                    <div className="bg-slate-950/60 p-1.5 rounded border border-slate-800/60">
                      <span className="text-purple-400 block font-bold">≥7.0</span>
                      <span className="text-slate-400">Mayor</span>
                    </div>
                  </div>
                </div>

                {/* Chart 2: Depth Profile */}
                <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 flex flex-col justify-between shadow-lg hover:border-slate-700/80 transition">
                  <div className="flex items-center justify-between mb-3 border-b border-slate-800 pb-3">
                    <div className="flex items-center space-x-2.5">
                      <span className="p-1.5 bg-amber-500/10 text-amber-400 rounded-lg">
                        <PieChart className="w-4 h-4" />
                      </span>
                      <div>
                        <h3 className="text-sm font-bold text-white">2. Perfil Hipocentral de Profundidad</h3>
                        <p className="text-[11px] text-slate-400">Clasificación geológica de la profundidad focal (km)</p>
                      </div>
                    </div>
                    <span className="text-[10px] px-2 py-0.5 bg-slate-800 text-slate-300 rounded font-mono">
                      Doughnut
                    </span>
                  </div>

                  <div className="h-64 w-full relative">
                    <canvas ref={depthChartRef}></canvas>
                  </div>

                  <div className="mt-3 pt-3 border-t border-slate-800 flex items-center justify-around text-[10px] text-slate-400">
                    <div className="flex items-center space-x-1.5">
                      <span className="w-2.5 h-2.5 rounded-full bg-red-500"></span>
                      <span>Superficial (&le;30km)</span>
                    </div>
                    <div className="flex items-center space-x-1.5">
                      <span className="w-2.5 h-2.5 rounded-full bg-amber-500"></span>
                      <span>Intermedio (30-100km)</span>
                    </div>
                    <div className="flex items-center space-x-1.5">
                      <span className="w-2.5 h-2.5 rounded-full bg-indigo-500"></span>
                      <span>Profundo (&gt;100km)</span>
                    </div>
                  </div>
                </div>

                {/* Chart 3: Temporal Series & Magnitude Trend */}
                <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 flex flex-col justify-between shadow-lg hover:border-slate-700/80 transition">
                  <div className="flex items-center justify-between mb-3 border-b border-slate-800 pb-3">
                    <div className="flex items-center space-x-2.5">
                      <span className="p-1.5 bg-emerald-500/10 text-emerald-400 rounded-lg">
                        <TrendingUp className="w-4 h-4" />
                      </span>
                      <div>
                        <h3 className="text-sm font-bold text-white">3. Evolución Temporal de Sismos y Magnitud</h3>
                        <p className="text-[11px] text-slate-400">Serie de tiempo con eje dual: Frecuencia horaria vs. Intensidad Media</p>
                      </div>
                    </div>
                    <span className="text-[10px] px-2 py-0.5 bg-emerald-500/10 text-emerald-400 border border-emerald-500/30 rounded font-mono">
                      Eje Dual
                    </span>
                  </div>

                  <div className="h-64 w-full relative">
                    <canvas ref={trendChartRef}></canvas>
                  </div>

                  <div className="mt-3 pt-3 border-t border-slate-800 flex items-center justify-between text-[11px] text-slate-400">
                    <span className="flex items-center space-x-1.5">
                      <span className="w-3 h-1 bg-emerald-500 rounded"></span>
                      <span>Volumen de Eventos</span>
                    </span>
                    <span className="flex items-center space-x-1.5">
                      <span className="w-3 h-0.5 border-t-2 border-dashed border-amber-500"></span>
                      <span>Magnitud Promedio (M)</span>
                    </span>
                  </div>
                </div>

                {/* Chart 4: Cumulative Seismic Energy Release */}
                <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 flex flex-col justify-between shadow-lg hover:border-slate-700/80 transition">
                  <div className="flex items-center justify-between mb-3 border-b border-slate-800 pb-3">
                    <div className="flex items-center space-x-2.5">
                      <span className="p-1.5 bg-rose-500/10 text-rose-400 rounded-lg">
                        <Zap className="w-4 h-4" />
                      </span>
                      <div>
                        <h3 className="text-sm font-bold text-white">4. Liberación de Energía Sísmica Acumulada</h3>
                        <p className="text-[11px] text-slate-400">Fórmula Gutenberg-Richter: log₁₀(E) = 4.8 + 1.5M (Megajoules)</p>
                      </div>
                    </div>
                    <span className="text-[10px] px-2 py-0.5 bg-rose-500/10 text-rose-400 border border-rose-500/30 rounded font-mono">
                      Curva de Energía
                    </span>
                  </div>

                  <div className="h-64 w-full relative">
                    <canvas ref={energyChartRef}></canvas>
                  </div>

                  <div className="mt-3 pt-3 border-t border-slate-800 flex items-center justify-between text-[11px] text-slate-400">
                    <span>Picos de liberación de esfuerzo cortical</span>
                    <span className="text-rose-400 font-mono">Unidad: MJ</span>
                  </div>
                </div>

                {/* Chart 5: Top 5 Active Regions */}
                <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 flex flex-col justify-between shadow-lg hover:border-slate-700/80 transition">
                  <div className="flex items-center justify-between mb-3 border-b border-slate-800 pb-3">
                    <div className="flex items-center space-x-2.5">
                      <span className="p-1.5 bg-sky-500/10 text-sky-400 rounded-lg">
                        <MapPin className="w-4 h-4" />
                      </span>
                      <div>
                        <h3 className="text-sm font-bold text-white">5. Top 5 Regiones Sísmicas Más Activas</h3>
                        <p className="text-[11px] text-slate-400">Zonas geográficas con mayor recurrencia de hipocentros</p>
                      </div>
                    </div>
                    <span className="text-[10px] px-2 py-0.5 bg-slate-800 text-slate-300 rounded font-mono">
                      Ranking Territorial
                    </span>
                  </div>

                  <div className="h-64 w-full relative">
                    <canvas ref={regionChartRef}></canvas>
                  </div>

                  <div className="mt-3 pt-3 border-t border-slate-800 flex items-center justify-between text-[11px] text-slate-400">
                    <span>Normalización territorial USGS</span>
                    <span className="text-sky-400 font-mono">Top Actividad</span>
                  </div>
                </div>

                {/* Chart 6: Hypocentral Depth vs Magnitude Scatter */}
                <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 flex flex-col justify-between shadow-lg hover:border-slate-700/80 transition">
                  <div className="flex items-center justify-between mb-3 border-b border-slate-800 pb-3">
                    <div className="flex items-center space-x-2.5">
                      <span className="p-1.5 bg-red-500/10 text-red-400 rounded-lg">
                        <Target className="w-4 h-4" />
                      </span>
                      <div>
                        <h3 className="text-sm font-bold text-white">6. Dispersión Profundidad vs. Magnitud</h3>
                        <p className="text-[11px] text-slate-400">Matriz geológica de hipocentros (Eje Y invertido hacia la corteza)</p>
                      </div>
                    </div>
                    <span className="text-[10px] px-2 py-0.5 bg-red-500/10 text-red-400 border border-red-500/30 rounded font-mono">
                      Scatter Plot
                    </span>
                  </div>

                  <div className="h-64 w-full relative">
                    <canvas ref={scatterChartRef}></canvas>
                  </div>

                  <div className="mt-3 pt-3 border-t border-slate-800 flex items-center justify-between text-[11px] text-slate-400">
                    <span>Eje vertical invertido: mayor profundidad hacia abajo</span>
                    <span className="text-red-400 font-mono">Matriz Hipocentros</span>
                  </div>
                </div>

                {/* Chart 7: Diurnal Activity Cycle */}
                <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 flex flex-col justify-between shadow-lg hover:border-slate-700/80 transition">
                  <div className="flex items-center justify-between mb-3 border-b border-slate-800 pb-3">
                    <div className="flex items-center space-x-2.5">
                      <span className="p-1.5 bg-purple-500/10 text-purple-400 rounded-lg">
                        <SunMedium className="w-4 h-4" />
                      </span>
                      <div>
                        <h3 className="text-sm font-bold text-white">7. Ciclo Diurno de Sismicidad (00h - 23h UTC)</h3>
                        <p className="text-[11px] text-slate-400">Distribución de hipocentros por hora del día en tiempo universal</p>
                      </div>
                    </div>
                    <span className="text-[10px] px-2 py-0.5 bg-purple-500/10 text-purple-400 border border-purple-500/30 rounded font-mono">
                      Ciclo 24h UTC
                    </span>
                  </div>

                  <div className="h-64 w-full relative">
                    <canvas ref={diurnalChartRef}></canvas>
                  </div>

                  <div className="mt-3 pt-3 border-t border-slate-800 flex items-center justify-between text-[11px] text-slate-400">
                    <span>Concentración temporal según rotación terrestre</span>
                    <span className="text-purple-400 font-mono">Distribución Horaria</span>
                  </div>
                </div>

                {/* Chart 8: Significance and Severity Levels */}
                <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 flex flex-col justify-between shadow-lg hover:border-slate-700/80 transition">
                  <div className="flex items-center justify-between mb-3 border-b border-slate-800 pb-3">
                    <div className="flex items-center space-x-2.5">
                      <span className="p-1.5 bg-emerald-500/10 text-emerald-400 rounded-lg">
                        <Shield className="w-4 h-4" />
                      </span>
                      <div>
                        <h3 className="text-sm font-bold text-white">8. Niveles de Severidad y Significancia USGS</h3>
                        <p className="text-[11px] text-slate-400">Score de impacto ponderado por población, magnitud e instrumentación</p>
                      </div>
                    </div>
                    <span className="text-[10px] px-2 py-0.5 bg-slate-800 text-slate-300 rounded font-mono">
                      USGS Impact Score
                    </span>
                  </div>

                  <div className="h-64 w-full relative">
                    <canvas ref={significanceChartRef}></canvas>
                  </div>

                  <div className="mt-3 pt-3 border-t border-slate-800 flex items-center justify-around text-[10px] text-slate-400">
                    <div className="flex items-center space-x-1.5">
                      <span className="w-2.5 h-2.5 rounded-full bg-emerald-500"></span>
                      <span>Leve (&lt;100)</span>
                    </div>
                    <div className="flex items-center space-x-1.5">
                      <span className="w-2.5 h-2.5 rounded-full bg-blue-500"></span>
                      <span>Mod. (100-299)</span>
                    </div>
                    <div className="flex items-center space-x-1.5">
                      <span className="w-2.5 h-2.5 rounded-full bg-amber-500"></span>
                      <span>Alta (300-599)</span>
                    </div>
                    <div className="flex items-center space-x-1.5">
                      <span className="w-2.5 h-2.5 rounded-full bg-red-500"></span>
                      <span>Crítica (≥600)</span>
                    </div>
                  </div>
                </div>
              </div>

              {/* Tarjetero del Informe Ejecutivo de IA (Narrativa Oficial de la Jornada / Ventana) */}
              <div className="bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-xl space-y-4">
                <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 border-b border-slate-800 pb-4">
                  <div className="flex items-center space-x-3">
                    <div className="p-2 bg-gradient-to-br from-emerald-500/20 to-sky-500/20 border border-emerald-500/30 rounded-xl text-emerald-400">
                      <Sparkles className="w-5 h-5" />
                    </div>
                    <div>
                      <div className="flex items-center space-x-2">
                        <h3 className="text-base font-bold text-white">
                          Informe Ejecutivo Consolidado - Ventana {analyticsData?.timeframe_label?.toUpperCase() || reportTimeframe.toUpperCase()}
                        </h3>
                        <span className="px-2 py-0.5 bg-slate-800 text-slate-300 rounded text-xs font-mono">
                          {analyticsData?.generated_at?.slice(0, 10) || new Date().toISOString().slice(0, 10)}
                        </span>
                      </div>
                      <p className="text-xs text-slate-400">
                        Síntesis adaptativa generada por el Agente de IA con control estricto de alucinaciones y validación matemática contra Gold.
                      </p>
                    </div>
                  </div>

                  <div className="flex items-center space-x-2">
                    <span className="px-2.5 py-1 bg-emerald-500/10 text-emerald-400 border border-emerald-500/30 rounded-lg text-xs font-mono font-medium flex items-center space-x-1.5">
                      <CheckCircle2 className="w-3.5 h-3.5" />
                      <span>Cifras 100% Verificadas</span>
                    </span>
                  </div>
                </div>

                {/* Narrative text block */}
                <div className="bg-slate-950/80 border border-slate-800/90 rounded-xl p-5 relative overflow-hidden">
                  <div className="absolute top-0 left-0 w-1 h-full bg-emerald-500"></div>
                  <h4 className="text-xs font-semibold text-emerald-400 uppercase tracking-wider mb-2 flex items-center space-x-1.5">
                    <FileText className="w-3.5 h-3.5" />
                    <span>Síntesis Ejecutiva Oficial ({reportTimeframe.toUpperCase()})</span>
                  </h4>
                  <p className="text-sm text-slate-200 leading-relaxed font-sans">
                    {analyticsData?.ai_narrative || activeReport?.summary_narrative || 'Cargando síntesis ejecutiva...'}
                  </p>
                </div>

                {/* Detailed metrics audit table */}
                <div className="bg-slate-950/50 border border-slate-800/60 rounded-xl p-4">
                  <h4 className="text-xs font-semibold text-slate-300 uppercase tracking-wider mb-3">
                    Auditoría Numérica de Hechos (Gold Lakehouse Layer)
                  </h4>
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-xs">
                    <div className="p-3 bg-slate-900/60 rounded-lg border border-slate-800">
                      <span className="text-slate-400 block text-[11px]">Eventos Consolidados</span>
                      <span className="text-white font-mono font-bold text-base mt-1 block">
                        {analyticsData?.summary?.total_events ?? reportMetrics.total_events}
                      </span>
                    </div>
                    <div className="p-3 bg-slate-900/60 rounded-lg border border-slate-800">
                      <span className="text-slate-400 block text-[11px]">Magnitud Promedio</span>
                      <span className="text-amber-400 font-mono font-bold text-base mt-1 block">
                        {analyticsData?.summary?.avg_magnitude != null
                          ? Number(analyticsData.summary.avg_magnitude).toFixed(2)
                          : (reportMetrics.avg_magnitude ? Number(reportMetrics.avg_magnitude).toFixed(2) : '0.00')} M
                      </span>
                    </div>
                    <div className="p-3 bg-slate-900/60 rounded-lg border border-slate-800">
                      <span className="text-slate-400 block text-[11px]">Sismo de Mayor Intensidad</span>
                      <span className="text-red-400 font-mono font-bold text-base mt-1 block truncate" title={analyticsData?.summary?.max_mag_place || reportMetrics.max_mag_place}>
                        {analyticsData?.summary?.max_magnitude != null
                          ? Number(analyticsData.summary.max_magnitude).toFixed(1)
                          : (reportMetrics.max_magnitude ? Number(reportMetrics.max_magnitude).toFixed(1) : '0.0')} M ({analyticsData?.summary?.max_mag_place || reportMetrics.max_mag_place || 'N/A'})
                      </span>
                    </div>
                    <div className="p-3 bg-slate-900/60 rounded-lg border border-slate-800">
                      <span className="text-slate-400 block text-[11px]">Energía Sísmica Estimada</span>
                      <span className="text-rose-400 font-mono font-bold text-base mt-1 block">
                        {analyticsData?.summary?.total_energy_mj != null
                          ? `${Number(analyticsData.summary.total_energy_mj).toLocaleString()} MJ`
                          : '0 MJ'}
                      </span>
                    </div>
                  </div>
                </div>
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
                <p className="text-xs text-slate-400">Acciones privilegiadas del sistema y sincronización histórica.</p>
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
