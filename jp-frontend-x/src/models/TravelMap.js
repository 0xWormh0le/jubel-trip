import { action, computed, decorate, observable, toJS } from 'mobx';
import { FlyToInterpolator } from 'react-map-gl';
import { ScaleControl } from 'mapbox-gl';
import isEqual from 'lodash/isEqual';
import WebMercatorViewport from 'viewport-mercator-project';

import ToggleState from './ToggleState';

import {
  getBoundsFromDestinations,
  getBoundsFromTrips,
  getBoundsOfCountry,
  getBoundsOfDestinationFromTrips,
  getDestinationsGeoJson,
  getGeoJSONForDestination,
  getGeoJSONForTrips,
} from '../utils/geo';
import theme from '../design-system/theme';
import { rgbaArray } from '../design-system/theme/colors';
import { MapboxLayer } from '@deck.gl/mapbox';
import { GeoJsonLayer, IconLayer, TextLayer } from '@deck.gl/layers';

const ROUTE_LINE_COLOR = rgbaArray(theme.colors.misc.map.route.normal);
const SAT_ROUTE_LINE_COLOR = rgbaArray(theme.colors.misc.map.route.satellite);
const ON_ROUTE_LINE_COLOR = rgbaArray(theme.colors.misc.map.on.route.normal);
const SAT_ON_ROUTE_LINE_COLOR = rgbaArray(
  theme.colors.misc.map.on.route.satellite
);
const MARKER_COLOR = rgbaArray(theme.colors.misc.map.marker.normal);
const SAT_MARKER_COLOR = rgbaArray(theme.colors.misc.map.marker.satellite);
const SAT_STYLE = process.env.REACT_APP_MAPBOX_STYLE_SATELLITE;
const NORMAL_STYLE = process.env.REACT_APP_MAPBOX_STYLE_NORMAL;

const GEOJSON_COUNTRY_LAYER_ID = 'geo-json-country-layer';
const GEOJSON_LINES_LAYER_ID = 'geo-json-lines-layer';
const GEOJSON_POINTS_LAYER_ID = 'geo-json-points-layer';
const GEOJSON_TEXT_LAYER_ID = 'geo-json-text-layer';

const ICON_MAPPING = {
  marker: {
    x: 0,
    y: 0,
    width: 48,
    height: 48,
    mask: true,
    anchorY: 48,
  },
};

const BOUNDING_RECT_PADDING = 50;

const INITIAL_BOUNDING_RECT = {
  left: BOUNDING_RECT_PADDING,
  top: BOUNDING_RECT_PADDING,
  right: BOUNDING_RECT_PADDING,
  bottom: BOUNDING_RECT_PADDING,
};

class TravelMap {
  viewport = {
    zoom: 2,
    longitude: -40,
    latitude: 20,
    pitch: 0,
    bearing: 0,
    maxZoom: 16,
    transitionDuration: 1500,
    transitionInterpolator: new FlyToInterpolator(),
  };

  placeName = '';
  placeValue = '';
  geoJson;
  map;
  satelliteStyleToggle = new ToggleState('satellite style');
  initialized = false;
  layers = [];
  gl = null;
  deck = null;
  hover = null;
  boundingRect = INITIAL_BOUNDING_RECT;

  markerClickHandler = null;

  addScaleControl() {
    this.map.addControl(new ScaleControl(), 'top-right');
  }

  setBoundingRect(rect) {
    this.boundingRect = {
      left: (rect.left || 0) + BOUNDING_RECT_PADDING,
      top: (rect.top || 0) + BOUNDING_RECT_PADDING,
      right: (rect.right || 0) + BOUNDING_RECT_PADDING,
      bottom: (rect.bottom || 0) + BOUNDING_RECT_PADDING,
    };
  }

  setMarkerClickHandler(handler) {
    this.markerClickHandler = handler;
    this.refreshLayers();
  }

  refreshLayers = () => {
    const { country, lines, points, point } = toJS(this.geoJson) || {};
    const { features } = points || {};
    const layers = [];
    if (country) {
      layers.push(
        new GeoJsonLayer({
          id: GEOJSON_COUNTRY_LAYER_ID,
          data: country,
          pickable: true,
          stroked: true,
          filled: true,
          extruded: true,
          lineWidthScale: 1,
          lineWidthMinPixels: 4,
          getFillColor: this.satelliteStyleToggle.isOpen
            ? SAT_ROUTE_LINE_COLOR
            : ROUTE_LINE_COLOR,
          getLineColor: this.satelliteStyleToggle.isOpen
            ? SAT_ROUTE_LINE_COLOR
            : ROUTE_LINE_COLOR,
          getLineWidth: 4,
          getElevation: 1,
        })
      );
    } else if (point) {
      // Single destination
      layers.push(
        new IconLayer({
          data: [point],
          pickable: true,
          iconAtlas: '/images/marker-icon.svg',
          iconMapping: ICON_MAPPING,
          getIcon: (d) => 'marker',
          getSize: 48,
          getPosition: (d) => d.geometry.coordinates,
          getColor: this.satelliteStyleToggle.isOpen
            ? SAT_MARKER_COLOR
            : MARKER_COLOR,
          onHover: this.handleHover,
        })
      );
    } else if (lines) {
      // Add path lines for current trip
      layers.push(
        new GeoJsonLayer({
          id: GEOJSON_LINES_LAYER_ID,
          data: lines,
          pickable: true,
          stroked: true,
          filled: true,
          extruded: true,
          lineWidthScale: 1,
          lineWidthMinPixels: 4,
          getFillColor: this.satelliteStyleToggle.isOpen
            ? SAT_ROUTE_LINE_COLOR
            : ROUTE_LINE_COLOR,
          getLineColor: this.satelliteStyleToggle.isOpen
            ? SAT_ROUTE_LINE_COLOR
            : ROUTE_LINE_COLOR,
          getLineWidth: 4,
          getElevation: 1,
        })
      );
    }
    if (points) {
      if (lines) {
        // Add circles for the destinations of the current trip
        layers.push(
          new GeoJsonLayer({
            id: GEOJSON_POINTS_LAYER_ID,
            data: points,
            pickable: true,
            stroked: true,
            filled: true,
            lineWidthScale: 1,
            lineWidthMinPixels: 4,
            pointRadiusMinPixels: 15,
            pointRadiusMaxPixels: 15,
            getFillColor: this.satelliteStyleToggle.isOpen
              ? SAT_ROUTE_LINE_COLOR
              : ROUTE_LINE_COLOR,
            getLineColor: this.satelliteStyleToggle.isOpen
              ? SAT_ON_ROUTE_LINE_COLOR
              : ON_ROUTE_LINE_COLOR,
            getRadius: 15,
            getLineWidth: 3,
            getElevation: 0,
            onHover: this.handleHover,
            onClick: this.markerClickHandler,
          })
        );
        if (features) {
          // Add numbered texts for the destinations of the current trip
          layers.push(
            new TextLayer({
              id: GEOJSON_TEXT_LAYER_ID,
              data: features,
              getPosition: (d) => d.geometry.coordinates,
              getText: (d) => String(d.properties.number),
              fontFamily: theme.fontFamily,
              fontWeight: 'bold',
              getSize: 32,
              getAngle: 0,
              getTextAnchor: 'middle',
              getAlignmentBaseline: 'center',
              getColor: this.satelliteStyleToggle.isOpen
                ? SAT_ON_ROUTE_LINE_COLOR
                : ON_ROUTE_LINE_COLOR,
              fp64: true,
            })
          );
        }
      } else if (features) {
        // Add markers for trip list
        layers.push(
          new IconLayer({
            data: features,
            pickable: true,
            iconAtlas: '/images/marker-icon.svg',
            iconMapping: ICON_MAPPING,
            getIcon: (d) => 'marker',
            getSize: 48,
            getPosition: (d) => d.geometry.coordinates,
            getColor: this.satelliteStyleToggle.isOpen
              ? SAT_MARKER_COLOR
              : MARKER_COLOR,
            onHover: this.handleHover,
            onClick: this.markerClickHandler,
          })
        );
      }
    }
    this.layers = layers;
    const beforeLayerID = this.getFirstSymbolLayerID();
    if (this.map) {
      this.layers.forEach(({ id }) => {
        if (!this.map.getLayer(id)) {
          this.map.addLayer(
            new MapboxLayer({ id, deck: this.deck }),
            beforeLayerID
          );
        }
      });
    }
  };

  get mapStyle() {
    return this.satelliteStyleToggle.isOpen ? SAT_STYLE : NORMAL_STYLE;
  }

  get width() {
    return this.map ? this.map.getCanvas().clientWidth : window.innerWidth;
  }

  get height() {
    return this.map ? this.map.getCanvas().clientHeight : window.innerHeight;
  }

  handleHover = ({ object, x, y }) => {
    if (object) {
      this.hover = { ...object, x, y };
    } else {
      this.hover = null;
    }
  };

  setGl = (gl) => {
    this.gl = gl;
  };

  setDeck = (deck) => {
    this.deck = deck;
  };

  setGeoJson(geoJson) {
    this.geoJson = geoJson;
    this.refreshLayers();
  }

  toggleSatStyle = () => {
    this.satelliteStyleToggle.toggle();
  };

  getFirstSymbolLayerID() {
    if (this.map) {
      const mapLayers = this.map.getStyle().layers;
      const firstSymbolLayer = mapLayers.find(
        (layer) => layer.type === 'symbol'
      );
      return firstSymbolLayer ? firstSymbolLayer.id : null;
    }
    return null;
  }

  initMap(event) {
    this.map = event.target;
    this.initialized = true;
    this.map.on('style.load', this.refreshLayers);
  }

  boundsToCenterZoom(bounds) {
    let zoom = 11.5; // incase bounds are just a single point, assign default zoom level first.
    if (!isEqual(bounds[0], bounds[1])) {
      const wmv1 = new WebMercatorViewport({
        width: this.width,
        height: this.height,
      });
      try {
        const vp = wmv1.fitBounds(bounds, {
          padding: this.boundingRect,
        });
        zoom = vp.zoom;
      } catch (ex) {
        // the latitude or longitude is invalid.
      }
    }
    const wmv2 = new WebMercatorViewport({
      width: this.width,
      height: this.height,
      zoom,
    });
    const [longitude, latitude] = wmv2.getMapCenterByLngLatPosition({
      lngLat: [
        (bounds[0][0] + bounds[1][0]) / 2,
        (bounds[0][1] + bounds[1][1]) / 2,
      ],
      pos: [
        (this.width - this.boundingRect.right + this.boundingRect.left) / 2,
        (this.height - this.boundingRect.bottom + this.boundingRect.top) / 2,
      ],
    });

    return { longitude, latitude, zoom };
  }

  updateViewport = (viewport) => {
    this.hover = null;
    this.viewport = { ...toJS(this.viewport), ...viewport };
  };

  nextPlace = (trip, boundingRect) => {
    this.setBoundingRect(boundingRect);
    const destinations = toJS(trip.destinations);
    const destIdx = Math.floor(Math.random() * destinations.length);
    const destination = destinations[destIdx];
    this.placeValue = destination.name;
    this.placeName = `${destination.name}, ${destination.countryName}`;
    this.setGeoJson(getGeoJSONForDestination(destination));
    this.updateViewport({
      ...this.boundsToCenterZoom(
        getBoundsOfDestinationFromTrips(destination, trip)
      ),
      bearing: 0,
      pitch: 0,
      transitionDuration: 1500,
    });
  };

  resetMap() {
    this.setBoundingRect(INITIAL_BOUNDING_RECT);
    this.setGeoJson({});
    this.updateViewport({
      zoom: 0,
      bearing: 0,
      pitch: 0,
      transitionDuration: 1000,
    });
  }

  flyToTrip(trip, boundingRect) {
    if (trip) {
      if (boundingRect) {
        this.setBoundingRect(toJS(boundingRect));
      }
      setTimeout(() => {
        const dest = toJS(trip.destinations);
        this.setGeoJson(getDestinationsGeoJson(dest));
        this.updateViewport({
          ...this.boundsToCenterZoom(getBoundsFromDestinations(dest)),
          bearing: 0,
          pitch: 0,
          transitionDuration: 1000,
        });
      }, 500);
    }
  }

  flyToDestination(destination, boundingRect, trip) {
    if (boundingRect) {
      this.setBoundingRect(boundingRect);
    }
    setTimeout(() => {
      this.setGeoJson(getGeoJSONForDestination(destination));
      this.updateViewport({
        ...this.boundsToCenterZoom(
          getBoundsOfDestinationFromTrips(destination, trip)
        ),
        bearing: 0,
        pitch: 0,
        transitionDuration: 1000,
      });
    }, 500);
  }

  setBoundsToTrips = (trips, boundingRect) => {
    if (trips && trips.length > 0) {
      if (boundingRect) {
        this.setBoundingRect(boundingRect);
      }
      this.setGeoJson(getGeoJSONForTrips(trips));
      this.updateViewport({
        ...this.boundsToCenterZoom(getBoundsFromTrips(trips)),
        bearing: 0,
        pitch: 0,
        transitionDuration: 1500,
      });
    }
  };

  setBoundsToCountry = (country, boundingRect) => {
    if (boundingRect) {
      this.setBoundingRect(boundingRect);
    }
    const countryBounds = getBoundsOfCountry(country);
    this.setGeoJson(countryBounds);
    setTimeout(() => {
      this.updateViewport({
        ...this.boundsToCenterZoom(countryBounds.country.geometry.coordinates),
        bearing: 0,
        pitch: 0,
        transitionDuration: 500,
      });
    }, 500);
  };
}

export default decorate(TravelMap, {
  gl: observable,
  // deck: observable,
  map: observable,
  hover: observable,
  viewport: observable,
  placeName: observable,
  placeValue: observable,
  geoJson: observable,
  satelliteStyleToggle: observable,
  initialized: observable,
  layers: observable,
  refreshLayers: action,
  handleHover: action,
  initMap: action,
  setGl: action,
  setMarkerClickHandler: action,
  setGeoJson: action,
  toggleSatStyle: action,
  setDeck: action,
  addScaleControl: action,
  boundsToCenterZoom: action,
  mapStyle: computed,
  updateViewport: action,
  nextPlace: action,
  resetMap: action,
  flyToTrip: action,
  flyToDestination: action,
  setBoundsToTrips: action,
  setBoundsToCountry: action,
});
