import { inject, observer } from 'mobx-react';
import { NavigationControl, StaticMap } from 'react-map-gl';
import { withTranslation } from 'react-i18next';
import compose from 'lodash/fp/compose';
import DeckGL from '@deck.gl/react';
import React from 'react';

import { Absolute, CheckBox, MaxPanel, Flex, Text } from '../design-system';
import system from '../utils/System';

import '../styles/mapbox-gl.css';

const mapboxApiAccessToken = process.env.REACT_APP_MAPBOX_TOKEN;

const TravelMapPopup = system(
  'TravelMapPopup',
  {
    extend: Absolute,
    p: 3,
    maxWidth: 250,
    bg: 'surface',
    borderRadius: 5,
    boxShadow: 1,
    mt: 3,
    zIndex: 1,
  },
  {
    transform: 'translateX(-50%)',
  }
);

export const TravelMap = ({
  setDeck,
  setGl,
  initMap,
  gl,
  wantsControls,
  viewport,
  onViewportChange,
  satOn,
  toggleSatStyle,
  mapStyle,
  layers,
  hover,
  t,
}) => {
  return (
    <MaxPanel className="travelMap" position="relative" height="100%">
      <DeckGL
        layers={layers}
        onViewStateChange={({ viewState }) => onViewportChange(viewState)}
        viewState={viewport}
        controller
        getCursor={({ isDragging }) =>
          hover ? 'pointer' : isDragging ? 'grabbing' : 'grab'
        }
        ref={(ref) => setDeck(ref && ref.deck)}
        onWebGLInitialized={setGl}
      >
        {gl && (
          <StaticMap
            gl={gl}
            mapStyle={mapStyle}
            reuseMaps
            mapboxApiAccessToken={mapboxApiAccessToken}
            preventStyleDiffing
            onLoad={initMap}
            attributionControl={false}
          >
            {wantsControls && (
              <Absolute bottom={30} right={55} zIndex={1}>
                <NavigationControl onViewportChange={onViewportChange} />
              </Absolute>
            )}
          </StaticMap>
        )}
        {hover && (
          <>
            <TravelMapPopup left={hover.x} top={hover.y}>
              <Text center fontSize={4}>
                {hover.properties.name}
              </Text>
            </TravelMapPopup>
          </>
        )}
      </DeckGL>
      {wantsControls && (
        <Absolute
          className="TravelMap__satellite-ctrl"
          borderRadius="sm"
          boxShadow={2}
          bottom={30}
          right={100}
          bg="surface"
          p={2}
        >
          <Flex>
            <CheckBox checked={satOn} onChange={toggleSatStyle} fontSize={3}>
              {t('map_satellite_checkbox', 'Satellite')}
            </CheckBox>
          </Flex>
        </Absolute>
      )}
    </MaxPanel>
  );
};

export default compose(
  withTranslation('common'),
  inject(({ app }) => ({
    setDeck: app.map.setDeck,
    setGl: app.map.setGl,
    initMap: (event) => {
      app.map.initMap(event);
      app.onMapLoad();
    },
    gl: app.map.gl,
    layers: app.map.layers,
    hover: app.map.hover,
    mapStyle: app.map.mapStyle,
    satOn: app.map.satelliteStyleToggle.isOpen,
    toggleSatStyle: app.map.toggleSatStyle,
    viewport: app.map.viewport,
    onViewportChange: app.map.updateViewport,
    wantsControls: app.isTripScreen,
  })),
  observer
)(TravelMap);
