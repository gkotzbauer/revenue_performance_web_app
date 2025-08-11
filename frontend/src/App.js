import React from "react";
import { Routes, Route, Link } from "react-router-dom";
import Dashboard from "./pages/Dashboard";
import Upload from "./pages/Upload";
import HelpGuide from "./components/HelpGuide";
import "./App.css";

function App() {
  return (
    <div className="App">
      <header className="App-header">
        <h1>📊 Revenue Performance Web App</h1>
        <nav>
          <ul className="nav-links">
            <li>
              <Link to="/">Dashboard</Link>
            </li>
            <li>
              <Link to="/upload">Upload Data</Link>
            </li>
            <li>
              <Link to="/help">Help / ML Guide</Link>
            </li>
          </ul>
        </nav>
      </header>

      <main>
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/upload" element={<Upload />} />
          <Route path="/help" element={<HelpGuide />} />
        </Routes>
      </main>

      <footer>
        <p>
          &copy; {new Date().getFullYear()} Revenue Performance Analytics — All rights reserved.
        </p>
      </footer>
    </div>
  );
}

export default App;