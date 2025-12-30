import React, { useState, useRef, useEffect } from 'react';
import Globe from 'react-globe.gl';
import * as THREE from 'three';

// Custom color palette (subset needed for globe)
const saladPalette = {
  green: 'rgb(83,166,38)',
  darkGreen: 'rgb(31,79,34)',
  midGreen: 'rgb(120,200,60)',
};

export default function GlobeComponent({ theme, themeMode, cityData }) {
  if (!theme) {
    return null; // Don't render if theme is not available yet
  }
  const globeContainerRef = useRef(null);
  const globeNetworkRef = useRef();

  // --- Globe View Persistence ---
  const getInitialGlobeView = () => {
    try {
      const saved = localStorage.getItem('globeView');
      if (saved) {
        return JSON.parse(saved);
      }
    } catch (e) {
      console.error('Failed to parse globe view from localStorage', e);
    }
    // Default: more zoomed in, centered
    return { lat: 20, lng: 0, altitude: 1.7 };
  };

  const [globeView, setGlobeView] = useState(getInitialGlobeView);

  // Update globe view state without writing to localStorage
  const handleGlobeViewChange = (view) => {
    setGlobeView(view);
  };

  // Save globe view to localStorage when user releases mouse/touch
  useEffect(() => {
    const globeEl = globeContainerRef.current;
    if (!globeEl) return;

    const saveView = () => {
      try {
        const currentView = globeNetworkRef.current.pointOfView();
        localStorage.setItem('globeView', JSON.stringify(currentView));
      } catch (e) {
        console.error('Failed to save globe view:', e);
      }
    };

    globeEl.addEventListener('mouseup', saveView);
    globeEl.addEventListener('touchend', saveView);

    return () => {
      globeEl.removeEventListener('mouseup', saveView);
      globeEl.removeEventListener('touchend', saveView);
    };
  }, []); // Run only once

  // Ensure globe background matches theme
  useEffect(() => {
    const scene = globeNetworkRef.current?.scene();
    if (scene) {
      scene.background = new THREE.Color(theme.palette.background.default);
    }
  }, [theme.palette.background.default]);

  // On first load, set globe to saved view
  useEffect(() => {
    if (globeNetworkRef.current) {
      globeNetworkRef.current.pointOfView(globeView, 0);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <div ref={globeContainerRef}>
      {/* Defensive: only render Globe if cityData is a non-empty array with valid GeoJSON */}
      {Array.isArray(cityData) && cityData.length > 0 && cityData.every(d => d.type === 'Feature') ? (
        <Globe
          ref={globeNetworkRef}
          width={480}
          height={400}
          globeImageUrl={themeMode === 'dark' ? '/earth-night.jpg' : '/earth-light.jpg'}
          backgroundColor={theme.palette.background.default}
          onPointOfViewChanged={handleGlobeViewChange}
          polygonsData={cityData}
          polygonAltitude={(d) => {
            // Use backend's normalized value for consistent altitude
            const normalized = d.properties.normalized || 0;
            return Math.max(0.002, normalized * 0.06);
          }}
          polygonCapColor={(d) => {
            // Simple color based on normalized value
            const intensity = d.properties.normalized || 0;
            if (themeMode === 'dark') {
              const alpha = Math.max(0.4, intensity);
              return `rgba(120, 200, 60, ${alpha})`;
            } else {
              const alpha = Math.max(0.5, intensity);
              return `rgba(83, 166, 38, ${alpha})`;
            }
          }}
          polygonSideColor={(d) => {
            const intensity = d.properties.normalized || 0;
            const alpha = Math.max(0.3, intensity * 0.8);
            return `rgba(31, 79, 34, ${alpha})`;
          }}
          polygonStrokeColor={() => '#00000000'}
          polygonsTransitionDuration={800}
          polygonLabel={(d) => `<div style="background: rgba(0,0,0,0.8); color: white; padding: 8px; border-radius: 4px; font-size: 12px;"><b>Nodes: ${d.properties.count}</b><br/>Hex: ${d.properties.hex.slice(0,8)}...</div>`}
          enablePointerInteraction={true}
          animateIn={false}
        />
      ) : (
        <div style={{ width: 480, height: 400, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          <span style={{ color: theme.palette.text.secondary }}>Loading globe data...</span>
        </div>
      )}
    </div>
  );
}
