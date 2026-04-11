import { useState } from "react";

export default function App() {
  const [count, setCount] = useState(0);

  return (
    <main className="app">
      <h1>Hosted on Amazon S3</h1>
      <p>Built with React + Vite, deployed via the catch-up CLI.</p>
      <div className="card">
        <button onClick={() => setCount((c) => c + 1)}>
          clicked {count} time{count === 1 ? "" : "s"}
        </button>
      </div>
      <footer>
        <small>served as a static website from an S3 bucket</small>
      </footer>
    </main>
  );
}
