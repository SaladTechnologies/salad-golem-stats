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
      <Globe
        ref={globeNetworkRef}
        width={480}
        height={400}
        globeImageUrl={themeMode === 'dark' ? '/earth-night.jpg' : '/earth-light.jpg'}
        backgroundColor={theme.palette.background.default}
        onPointOfViewChanged={handleGlobeViewChange}
        hexBinPointsData={cityData}
        hexBinPointLat={(d) => d.lat}
        hexBinPointLng={(d) => d.lon}
        hexBinResolution={3}
        hexAltitude={(d) => Math.min(0.15, d.sumWeight * 0.01)}
        hexTopColor={() => (themeMode === 'dark' ? saladPalette.midGreen : saladPalette.green)}
        hexSideColor={() => (themeMode === 'dark' ? saladPalette.midGreen : saladPalette.darkGreen)}
      />
    </div>
  );
}
