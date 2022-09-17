import { action, computed, decorate, observable } from 'mobx';
import { flatten, kebabCase } from 'lodash';
import { RouterStore } from 'mobx-react-router';

import { travelStyles, travelStylesByAbbr } from './TravelStyle';
import Customer from './Customer';
import GTMTracker from './GTMTracker';
import HubSpotTracker from './HubSpotTracker';
import Route from './Route';
import TabsState from './TabsState';
import ToggleState from './ToggleState';
import TravelMap from './TravelMap';
import WithRoot from './WithRoot';

import packageJson from '../../package.json';
import GoogleAnalyticsTracker from './GoogleAnalyticsTracker';
import addDays from 'date-fns/add_days';
const appName = packageJson.name;
const appVersion = packageJson.version;

export const locales = [
  { key: 'en-US', value: 'US' },
  { key: 'jp-JP', value: 'Japan' },
  { key: 'es-MX', value: 'Mexico' },
  { key: 'es-ES', value: 'Spain' },
  { key: 'de-DE', value: 'Germany' },
  { key: 'fr-FR', value: 'France' },
];

export default class AppState extends WithRoot {
  appName = appName;
  appVersion = appVersion;
  prefsWidths = [40, 360];
  tripListPanelWidth = 600;
  tripPanelWidth = 480;
  headerHeight = 60;
  countryPanelWidth = 600;
  scorableAltPanelWidth = 600;
  searchBoxHeight = 398;

  // TODO: Obsolete the following 3 variables
  summaryWidth = 533;
  routePanelWidths = [135, 424];
  tripCtrlPanelWidth = 300;

  locales = locales;

  gtmTracker = new GTMTracker({ appName, appVersion });
  hsTracker = new HubSpotTracker({ name: '_hsqFake' });
  gTracker = new GoogleAnalyticsTracker({ name: 'dataLayer' });

  history = null;

  menuToggle = new ToggleState('main menu');
  toggleSummary = new ToggleState('trip summary');
  toggleQuote = new ToggleState('get quote');

  route = new Route();

  customer = null;

  travelStylePanelToggle = new ToggleState('travel style');
  _activeTravelStyle = 'oc';

  map = new TravelMap();

  storyIndexToggle = new ToggleState({
    name: 'story index',
    open: true,
  });

  currentTrip = null;

  searchValue = null;
  showBaseMap = true;

  showMoreToggle = new ToggleState('load more');

  routing = new RouterStore();

  countryInfo = null;

  constructor() {
    super();
    this.customer = new Customer(this);
  }

  setTripsMode = () => {
    this.currentTrip = null;
    this.route.countryProfilePanelToggle.close();
    this.map.setBoundsToTrips(this.trips, this.boundingRect);
  };

  resetTripsMode = () => {
    this.setCurrentTrip(null);
    this.map.resetMap();
  };

  onMapLoad() {
    this.map.setMarkerClickHandler(this.handleMarkerClick);
  }

  get prefsWidth() {
    return this.prefsWidths[this.preferences.toggler.isOpen ? 1 : 0];
  }

  setSearchValue = (searchValue) => {
    this.searchValue = searchValue;
    this.triggerSearch();
  };

  triggerSearch() {
    this.preferences.resetMatchedTrips();
    this.map.resetMap();
    if (this.searchValue) {
      this.goto(`/trips?q=${encodeURIComponent(this.searchValue.value)}`);
    } else {
      this.goto('/trips');
    }
  }

  get preferences() {
    return this.customer.preferences;
  }

  get trips() {
    return this.preferences.matchedTrips;
  }

  get tripCount() {
    return this.trips.length;
  }

  get hasTrips() {
    return this.tripCount > 0;
  }

  get isTripScreen() {
    return this.routing && this.routing.location.pathname.startsWith('/trips');
  }

  get isTripListScreen() {
    return this.routing && this.routing.location.pathname === '/trips';
  }

  get isSearchScreen() {
    return this.routing && this.routing.location.pathname === '/';
  }

  setCurrentTrip = (trip) => {
    this.currentTrip = trip;
    this.selectedRouteElement = null;
    this.routeElementToggle.close();
    this.route.countryProfilePanelToggle.close();
    this.route.summaryToggle.close();
    this.route.panelExpandedToggle.open();
    if (trip) {
      this.map.flyToTrip(this.currentTrip, this.boundingRect);
    } else if (this.hasTrips) {
      this.map.setBoundsToTrips(this.trips, this.boundingRect);
    }
  };

  get routeSummary() {
    return this.currentTrip && this.route.summaryToggle.isOpen
      ? this.currentTrip.summary
      : [];
  }

  get routeDaily() {
    return this.currentTrip && this.route.summaryToggle.isClosed
      ? this.currentTrip.route
      : [];
  }

  get showMore() {
    return this.showMoreToggle.isOpen;
  }

  toggleShowMore() {
    this.showMoreToggle.toggle();
  }

  selectedRouteElement = null;
  routeElementToggle = new ToggleState('route element');

  get hasSelectedRouteElement() {
    return !!this.selectedRouteElement;
  }

  selectCountry = (tripId, countryCode) => {
    if (!this.currentTrip || this.currentTrip.id !== tripId) {
      this.currentTrip = this.preferences.getTripById(tripId);
    }
    if (this.currentTrip) {
      this.route.countryProfilePanelToggle.open();
      this.routeElementToggle.close();
      this.route.setCountryTabsState(countryCode, this.currentTrip);
      this.setBoundsToCountry(countryCode);
    }
  };

  selectDestination = (tripId, destinationId) => {
    if (!this.currentTrip || this.currentTrip.id !== tripId) {
      this.currentTrip = this.preferences.getTripById(tripId);
    }

    if (this.currentTrip) {
      this.route.countryProfilePanelToggle.close();
      this.selectedRouteElement = this.currentTrip.destinations.find(
        (dest) => dest.id === destinationId
      );
      this.routeElementToggle.open();
      this.map.flyToDestination(
        this.selectedRouteElement,
        this.boundingRect,
        this.currentTrip
      );
    }
  };

  handleMarkerClick = ({ object }) => {
    if (object.properties.kind === 'destination' && this.currentTrip) {
      const destinationId = object.properties.id;
      this.goto(`/trips/${this.currentTrip.id}/destination/${destinationId}`);
    } else if (object.properties.kind === 'trip') {
      const tripId = object.properties.id;
      this.goto(`/trips/${tripId}`);
    }
  };

  tripTabsState = new TabsState([
    { id: 'itinerary', name: 'Itinerary' },
    { id: 'profile', name: 'Profile' },
    { id: 'included', name: 'Included' },
  ]);

  // TODO: Obsolete the following 2 tabs states
  selectedRouteElementTabsState = new TabsState([
    { id: 'about', name: 'About' },
    { id: 'profile', name: 'Profile' },
    { id: 'alternatives', name: 'Alternatives' },
  ]);

  tripSummaryTabsState = new TabsState([
    { id: 'profile', name: 'Profile' },
    { id: 'whatsIncluded', name: "What's Included" },
  ]);

  travelStyle(abbr) {
    return travelStylesByAbbr[abbr] || null;
  }

  get travelStyles() {
    return travelStyles;
  }

  get otherTravelStyles() {
    return travelStyles.filter((s) => s.abbr !== this._activeTravelStyle);
  }

  get activeTravelStyle() {
    return this._activeTravelStyle
      ? this.travelStyle(this._activeTravelStyle)
      : null;
  }

  setActiveTravelStyle = (style) => {
    if (style === this._activeTravelStyle) this.travelStylePanelToggle.toggle();
    else {
      this._activeTravelStyle = style;
      if (this.travelStylePanelToggle.isClosed)
        this.travelStylePanelToggle.toggle();
    }
  };

  goto = (route) => {
    this.routing.history.push(route);
  };

  get isTripPanelOpen() {
    const hasTrip = Boolean(this.currentTrip);
    if (!this.routing) return false;
    const routeMatches = /trips\/(\d+)$/.test(this.routing.location.pathname);
    return hasTrip && routeMatches;
  }

  get isTripListPanelOpen() {
    return (
      this.routing &&
      this.routing.location.pathname === '/trips' &&
      this.trips.length > 0
    );
  }

  get isCountryProfilePanelOpen() {
    return this.route.countryProfilePanelToggle.isOpen;
  }

  initMap(map) {
    if (map) {
      this.map.initMap(map);
    }
  }

  trackScreenView = (name) => {
    this.gtmTracker.screenview(name);
    this.hsTracker.setPath('/' + kebabCase(name));
  };

  getCountryInfo(countryCode) {
    const DAYS_IN_CACHE = 1;

    const fetchCountryInfo = () => {
      return fetch(`${process.env.REACT_APP_API_URL}/countries/${countryCode}`)
        .then((response) => response.json())
        .then((json) => {
          const countryInfo = {
            ...json,
            description: json.description
              .split(/[\r\n]+/)
              .map((p) => `<p>${p}</p>`)
              .join('\n'),
            boundingBox: flatten(json.boundingBox).map((c) => Number(c)),
          };
          this.countryInfo = countryInfo;
          window.localStorage.setItem(
            countryCode,
            JSON.stringify({
              expirationDate: addDays(new Date(), DAYS_IN_CACHE),
              ...countryInfo,
            })
          );
          return countryInfo;
        })
        .catch((e) => {
          console.error(`Error fetching countryInfo for ${countryCode}`, e);
        });
    };

    if (countryCode && this.currentTrip) {
      const cached = JSON.parse(window.localStorage.getItem(countryCode));
      const expirationDate =
        cached &&
        cached.expirationDate &&
        new Date(cached.expirationDate).getTime();
      const cacheValid =
        expirationDate && expirationDate > new Date().getTime();

      if (cacheValid) {
        return new Promise((resolve) => {
          this.countryInfo = cached;
          resolve(cached);
        });
      } else {
        return fetchCountryInfo();
      }
    }
  }

  setBoundsToCountry = (countryCode) => {
    return this.getCountryInfo(countryCode).then((countryInfo) => {
      if (countryInfo.boundingBox.length === 4) {
        this.map.setBoundsToCountry(countryInfo, this.boundingRect);
      } else {
        console.error('Bounding box error', countryInfo);
      }
    });
  };

  setSearchBoxHeight = (height) => {
    this.searchBoxHeight = height;
  };

  get boundingRect() {
    let left = 0,
      right = 0,
      top = 0;
    if (this.isTripListPanelOpen) {
      left = this.tripListPanelWidth;
    } else if (this.isTripPanelOpen) {
      left = this.tripPanelWidth;
    } else if (this.route.countryProfilePanelToggle.isOpen) {
      left = this.countryPanelWidth;
    } else if (this.routeElementToggle.isOpen) {
      left = this.scorableAltPanelWidth;
    }
    if (this.isTripScreen) {
      if (this.isTripListScreen) {
        right = this.prefsWidths[0];
      }
      top = this.headerHeight;
    } else if (this.isSearchScreen) {
      top = this.searchBoxHeight + this.headerHeight;
    }
    return { left, top, right, bottom: 0 };
  }

  getCountryImages(countryCode) {
    if (this.currentTrip) {
      return this.currentTrip.destinations
        .filter((d) => d.countryCode === countryCode)
        .reduce((a, d) => a.concat(d.images), []);
    } else {
      return [];
    }
  }
}

decorate(AppState, {
  selectedRouteElement: observable,
  routeElementToggle: observable,
  menuToggle: observable,
  toggleSummary: observable,
  toggleQuote: observable,
  customer: observable,
  onMapLoad: action,
  travelStylePanelToggle: observable,
  _activeTravelStyle: observable,
  storyIndexToggle: observable,
  currentTrip: observable,
  searchValue: observable,
  showBaseMap: observable,
  setTripsMode: action,
  resetTripsMode: action,
  prefsWidth: computed,
  setSearchValue: action,
  triggerOnEnter: action,
  triggerSearch: action,
  preferences: computed,
  trips: computed,
  tripCount: computed,
  hasTrips: computed,
  setCurrentTrip: action,
  otherTravelStyles: computed,
  activeTravelStyle: computed,
  setActiveTravelStyle: action,
  goto: action,
  handleMarkerClick: action,
  routeSummary: computed,
  routeDaily: computed,
  hasSelectedRouteElement: computed,
  selectRouteElement: action,
  selectCountry: action,
  tripTabsState: observable,
  selectedRouteElementTabsState: observable,
  tripSummaryTabsState: observable,
  showMoreToggle: observable,
  showMore: computed,
  toggleShowMore: action,
  isTripPanelOpen: computed,
  isTripListPanelOpen: computed,
  map: observable,
  route: observable,
  setBoundsToCountry: action,
  countryInfo: observable,
  searchBoxHeight: observable,
  setSearchBoxHeight: action,
});
