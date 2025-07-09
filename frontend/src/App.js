import React, { useState } from 'react';
import axios from 'axios';
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
} from 'recharts';
import './App.css';

function hashUserId(userId) {
  const buffer = new TextEncoder().encode(userId);
  return crypto.subtle.digest('SHA-256', buffer).then(hash =>
    Array.from(new Uint8Array(hash)).map(b => b.toString(16).padStart(2, '0')).join('')
  );
}

function App() {
  const [form, setForm] = useState({ user_id: '', trade_type: '', amount: '', asset: '' });
  const [alerts, setAlerts] = useState([]);
  const [trades, setTrades] = useState([]);
  const [loading, setLoading] = useState(false);

  const fetchAlerts = async (idHash) => {
    try {
      const res = await axios.get(`http://localhost:8080/getAlerts/${idHash}`);
      setAlerts((res.data || []).reverse());
    } catch (err) {
      console.error("❌ Fetch alerts failed", err);
      setAlerts([]);
    }
  };

  const fetchTrades = async (idHash) => {
    try {
      const res = await axios.get(`http://localhost:8080/getTrades/${idHash}`);
      setTrades(res.data || []);
    } catch (err) {
      console.error("❌ Fetch trades failed", err);
      setTrades([]);
    }
  };

  const refreshDataAfterDelay = (idHash) => {
    setTimeout(() => {
      fetchAlerts(idHash);
      fetchTrades(idHash);
    }, 800);
  };

  const submitTrade = async (e) => {
    e.preventDefault();
    setLoading(true);
    const idHash = await hashUserId(form.user_id);

    try {
      await axios.post('http://localhost:8080/submitTrade', {
        ...form,
        amount: parseFloat(form.amount),
      });
      refreshDataAfterDelay(idHash);
    } catch (err) {
      console.error("❌ Trade submission failed", err);
    } finally {
      setLoading(false);
    }
  };

  const mean = trades.reduce((sum, t) => sum + t.amount, 0) / (trades.length || 1);
  const std = Math.sqrt(
    trades.reduce((sum, t) => sum + Math.pow(t.amount - mean, 2), 0) / (trades.length || 1)
  );

  return (
    <div className="container">
      <h1>📈 TradeFlow Sentry</h1>

      <form onSubmit={submitTrade}>
        <input placeholder="User ID" required value={form.user_id} onChange={e => setForm({ ...form, user_id: e.target.value })} />
        <input placeholder="Trade Type (buy/sell)" required value={form.trade_type} onChange={e => setForm({ ...form, trade_type: e.target.value })} />
        <input placeholder="Amount" type="number" required value={form.amount} onChange={e => setForm({ ...form, amount: e.target.value })} />
        <input placeholder="Asset (e.g., TSLA)" required value={form.asset} onChange={e => setForm({ ...form, asset: e.target.value })} />
        <button type="submit" disabled={loading}>
          {loading ? "Submitting..." : "Submit"}
        </button>
      </form>

      {alerts.length > 0 && (
        <section>
          <h2>🔔 Recent Alerts</h2>
          <ul className="alerts">
            {alerts.map((a, i) => (
              <li key={i}>
                <strong>{a.alert_msg}</strong><br />
                Score: {a.anomaly_score}
              </li>
            ))}
          </ul>
        </section>
      )}

      {trades.length > 0 && (
        <section>
          <h2>📊 Trade Summary</h2>
          <p>Avg: ${mean.toFixed(2)} | Std Dev: ${std.toFixed(2)}</p>
          <LineChart width={900} height={350} data={trades.map(t => ({
            ...t,
            timestamp: new Date(t.timestamp).toLocaleString()
          }))}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="timestamp" />
            <YAxis />
            <Tooltip />
            <Legend />
            <Line type="monotone" dataKey="amount" stroke="#4f46e5" name="Trade Amount" />
          </LineChart>
        </section>
      )}
    </div>
  );
}

export default App;
