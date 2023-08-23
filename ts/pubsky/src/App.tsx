import "./App.css";
import { RouterProvider, createBrowserRouter } from "react-router-dom";
import ViewThread from "./components/ViewThread/ViewThread";

const router = createBrowserRouter([
  {
    path: "/profile/:handleOrDid/post/:rkey",
    element: <ViewThread />,
  },
]);

function App() {
  return (
    <>
      <div className="min-h-screen bg-gray-900">
        <main>
          <div className="mx-auto max-w-7xl py-6 sm:px-6 lg:px-8">
            <RouterProvider router={router} />
          </div>
        </main>
      </div>
    </>
  );
}

export default App;
