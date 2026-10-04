import {
  BrowserRouter,
  Routes,
  Route,
} from "react-router-dom";

import Sidebar from "./components/Sidebar";
import Topbar from "./components/Topbar";

import Dashboard from "./pages/Dashboard";
import Meters from "./pages/Meters";
import MeterDetails from "./pages/MeterDetails";
import Anomalies from "./pages/Anomalies";
import Analytics from "./pages/Analytics";
import Alerts from "./pages/Alerts";

function App() {

  return (

    <BrowserRouter>

      <div className="app-layout">

        <Sidebar />

        <div className="main-content">

          <Topbar />

          <main className="page-content">

            <Routes>

              <Route
                path="/"
                element={<Dashboard />}
              />

              <Route
                path="/dashboard"
                element={<Dashboard />}
              />

              <Route
                path="/meters"
                element={<Meters />}
              />

              <Route
                path="/meters/:meterId"
                element={<MeterDetails />}
              />

              <Route
                path="/anomalies"
                element={<Anomalies />}
              />

              <Route
                path="/analytics"
                element={<Analytics />}
              />
              <Route
                path="/alerts"
                element={<Alerts />}
              />
            </Routes>

          </main>

        </div>

      </div>

    </BrowserRouter>
  );
}

export default App;