import { spawn, execSync } from "child_process";
import dotenv from "dotenv";

dotenv.config();

// 1. Install/Verify Python dependencies synchronously on start
try {
  console.log("🐍 SRE Loader: Checking and installing Python dependencies...");
  execSync("pip3 install streamlit plotly pandas requests kafka-python-ng kubernetes", { stdio: "inherit" });
} catch (err) {
  console.error("⚠️ SRE Loader: Could not run pip3 install automatically. Streamlit server might fail if dependencies are missing.", err);
}

// 2. Spawn the Streamlit server on Port 3000
console.log("🐍 SRE Loader: Spawning Streamlit web server (streamlit run app.py on port 3000)...");
const pythonProcess = spawn("streamlit", ["run", "app.py", "--server.port", "3000", "--server.address", "0.0.0.0", "--server.headless", "true"], {
  stdio: "inherit",
  env: { ...process.env }
});

pythonProcess.on("close", (code) => {
  console.error(`⚠️ SRE Loader: Streamlit process exited with code ${code}`);
});

