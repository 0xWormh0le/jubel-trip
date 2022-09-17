import { action, computed, observable, decorate } from 'mobx';
import Route from './Route';
import differenceInDays from 'date-fns/difference_in_days';
import formatDate from 'date-fns/format';

import { ImageSorter as ISorter } from './ImageSorter';

// const tabsMyTrips = [
//   { id: 'trips', name: 'Trips' },
//   { id: 'destinations', name: 'Destinations(14)' },
//   { id: 'activities', name: 'Activities(29)' },
// ];
// const tabs = [
//   { id: 'trips', name: 'Trips' },
//   { id: 'profile', name: 'Profile' },
// ];

const destinations = [
  {
    id: 1,
    name: 'Tusheti National Park',
    description: `<p>A magic city, more than 16 centuries old, Georgia's capital is full
of fascinating stories to tell. Back in the day, Persians, Ottoman Turks, Byzantine Greeks, 
Russians all came as enemies, but stayed as friends. Since its very beginning, the city has 
attracted people of all walks of life and welcomed people of contrasting cultures, different 
languages and religion; and by doing so, the city has enriched itself and ornamented the 
country with an immense diversity. </p>
<p>Tbilisi is sure to charm you with its grace and hospitality, something you’ll certainly 
experience upon interaction with its people. Stroll around the Old Town for a glimpse of the 
past, get lost in the little streets, wander through its fresh local markets, enjoy the 
picturesque architecture, indulge in the many inviting cafes, or absorb the history in the 
spruced-up museums. The nightlife is also teeming with young people. This is a new modern 
city that is slowly opening itself up to the world.</p>`,
    duration: '21 - 23 July',
    contentLocation: {
      geo: [45.2116553, 42.4043933],
      image:
        'https://images.unsplash.com/photo-1557870496-fa8f75b9dd78?ixlib=rb-1.2.1&q=80&fm=jpg&crop=entropy&cs=tinysrgb&w=630&h=560&fit=crop&ixid=eyJhcHBfaWQiOjF9',
      name: 'Tusheti National Park',
    },
    summary: {
      weather: 'Warm, sunny and dry',
      accommodation: 'Mid Range - Fabrika',
      logistics: 'Flight + Taxi',
      scenery: 'Mountains',
      activities:
        'Architecture, Churches, Cooking Classes, Day Trips, Hiking, Historical Sites, Market Visits, Museums, Nightlife',
    },
  },
  {
    id: 2,
    name: 'Madrid',
    description: `<p>A magic city, more than 16 centuries old, Georgia's capital is full
of fascinating stories to tell. Back in the day, Persians, Ottoman Turks, Byzantine Greeks, 
Russians all came as enemies, but stayed as friends. Since its very beginning, the city has 
attracted people of all walks of life and welcomed people of contrasting cultures, different 
languages and religion; and by doing so, the city has enriched itself and ornamented the 
country with an immense diversity. </p>
<p>Tbilisi is sure to charm you with its grace and hospitality, something you’ll certainly 
experience upon interaction with its people. Stroll around the Old Town for a glimpse of the 
past, get lost in the little streets, wander through its fresh local markets, enjoy the 
picturesque architecture, indulge in the many inviting cafes, or absorb the history in the 
spruced-up museums. The nightlife is also teeming with young people. This is a new modern 
city that is slowly opening itself up to the world.</p>`,
    duration: '24 - 25 July',
    contentLocation: {
      geo: [-3.8199622, 40.4378693],
      image:
        'https://images.unsplash.com/photo-1558162921-0f9c3fb8928b?ixlib=rb-1.2.1&q=80&fm=jpg&crop=entropy&cs=tinysrgb&w=635&h=560&fit=crop&ixid=eyJhcHBfaWQiOjF9',
      name: 'Madrid',
      tagline: 'The Balcony of Europe',
    },
    summary: {
      weather: 'Warm, sunny and dry',
      accommodation: 'Mid Range - Fabrika',
      logistics: 'Flight + Taxi',
      scenery: 'Mountains',
      activities:
        'Architecture, Churches, Cooking Classes, Day Trips, Hiking, Historical Sites, Market Visits, Museums, Nightlife',
    },
  },
  {
    id: 3,
    name: 'Barcelona',
    description: `<p>A magic city, more than 16 centuries old, Georgia's capital is full
of fascinating stories to tell. Back in the day, Persians, Ottoman Turks, Byzantine Greeks, 
Russians all came as enemies, but stayed as friends. Since its very beginning, the city has 
attracted people of all walks of life and welcomed people of contrasting cultures, different 
languages and religion; and by doing so, the city has enriched itself and ornamented the 
country with an immense diversity. </p>
<p>Tbilisi is sure to charm you with its grace and hospitality, something you’ll certainly 
experience upon interaction with its people. Stroll around the Old Town for a glimpse of the 
past, get lost in the little streets, wander through its fresh local markets, enjoy the 
picturesque architecture, indulge in the many inviting cafes, or absorb the history in the 
spruced-up museums. The nightlife is also teeming with young people. This is a new modern 
city that is slowly opening itself up to the world.</p>`,
    duration: '26 - 28 July',
    contentLocation: {
      geo: [2.0785567, 41.3947687],
      image:
        'https://images.unsplash.com/photo-1558997886-7ac2ee85ba1a?ixlib=rb-1.2.1&q=80&fm=jpg&crop=entropy&cs=tinysrgb&w=640&h=560&fit=crop&ixid=eyJhcHBfaWQiOjF9',
      name: 'Barcelona',
    },
    summary: {
      weather: 'Warm, sunny and dry',
      accommodation: 'Mid Range - Fabrika',
      logistics: 'Flight + Taxi',
      scenery: 'Mountains',
      activities:
        'Architecture, Churches, Cooking Classes, Day Trips, Hiking, Historical Sites, Market Visits, Museums, Nightlife',
    },
  },
];

export const tripMock = {
  name: 'Your 14 Day Journey Through Spain',
  departOnOrAfter: '2019-07-15',
  returnOnOrBefore: '2019-07-29',
  groupSize: 7,
  groupType: 'adults',
  estimatedPrice: 3150.0,
  rating: 3,
  comments: [{ message: 'comment1' }, { message: 'comment2' }],
  image:
    'https://images.unsplash.com/photo-1558408768-656fefbe5168?ixlib=rb-1.2.1&q=80&fm=jpg&crop=entropy&cs=tinysrgb&w=1280&h=720&fit=crop&ixid=eyJhcHBfaWQiOjF9',
  country: 'Geogia',
  tagline: 'The Balcony of Europe',
  destinations,
  chartData: [
    {
      name: 'Active Nature',
      value: 250,
    },
    {
      name: 'Oceanist',
      value: 200,
    },
    {
      name: 'Culturist',
      value: 200,
    },
    {
      name: 'Party Purist',
      value: 200,
    },
    {
      name: 'Chilled Nature',
      value: 150,
    },
  ],
  potentialShocks: [
    {
      name: 'English Level',
      value: 'Decent',
    },
    {
      name: 'Foodborne Illness',
      value: 'Medium Risk',
    },
    {
      name: 'Petty Crime',
      value: 'Extremely low',
    },
    {
      name: 'Local Infrastructure',
      value: 'Unreliable',
    },
  ],
  packages: [
    'Flights, accommodations & transfers',
    'Your Jubel Atlas:<br />A curated shortlist of insider recommendations for you to check out -including restaurants, bars, nightlife, attractions, and day trips. Common best practices and precautions are also shared so that you can safely enjoy your adventure.',
    'Customer support 24/7',
    'Concierge services',
    'All taxes & fees',
    'Changes at no extra cost (where possible)',
    'Money-back guarantee (restrictions apply)',
  ],
  costs: [
    { type: 'Hotels', value: 745 },
    { type: 'Flights (1 Stop, Economy)', value: 450 },
    { type: 'Rental Car (Manual)', value: 500 },
  ],
};

export default class Trip {
  heroImageSorter = null;

  currentDestination = null;

  constructor(tripId) {
    this.id = tripId;
    this.heroImageSorter = new ISorter({
      images: this.allImages,
    });
    this.currentDestination = destinations[0];
  }

  get name() {
    return tripMock.name;
  }

  get country() {
    return tripMock.country;
  }

  get tagline() {
    return tripMock.tagline;
  }

  get departOnOrAfter() {
    return formatDate(new Date(tripMock.departOnOrAfter), 'MMMM DD');
  }

  get returnOnOrBefore() {
    return formatDate(new Date(tripMock.returnOnOrBefore), 'MMMM DD');
  }

  get duration() {
    return `${this.departOnOrAfter} - ${this.returnOnOrBefore}`;
  }

  get nights() {
    return differenceInDays(
      new Date(tripMock.returnOnOrBefore),
      new Date(tripMock.departOnOrAfter)
    );
  }

  get estimatedPrice() {
    return tripMock.estimatedPrice;
  }

  get targetGroup() {
    return `${tripMock.groupSize} ${tripMock.groupType}`;
  }

  get route() {
    return new Route();
  }

  get rating() {
    return tripMock.rating;
  }

  get commentsCount() {
    return tripMock.comments ? tripMock.comments.length : 0;
  }

  get image() {
    return tripMock.image;
  }

  get destinations() {
    return tripMock.destinations || [];
  }

  get chartColors() {
    return ['#64e4a9', '#69a5f1', '#cd9ff8', '#f1de85', '#a2f48f'];
  }

  get chartData() {
    return tripMock.chartData.map((item) => ({
      x: item.name,
      y: item.value,
    }));
  }

  get potentialShocks() {
    return tripMock.potentialShocks;
  }

  get packages() {
    return tripMock.packages;
  }

  get costs() {
    return tripMock.costs;
  }

  get allImages() {
    return [
      {
        src: this.image,
        caption: this.name,
        srcSet: [this.image],
        thumbnail: this.image,
      },
      ...this.destinations.map((destination) => ({
        src: destination.contentLocation.image,
        caption: destination.contentLocation.name,
        srcSet: [destination.contentLocation.image],
        thumbnail: destination.contentLocation.image,
      })),
    ];
  }

  setDestination(destination) {
    this.currentDestination = destination;
  }
}

decorate(Trips, {
  heroImageSorter: observable,
  currentDestination: observable,
  name: computed,
  country: computed,
  tagline: computed,
  departOnOrAfter: computed,
  returnOnOrBefore: computed,
  duration: computed,
  nights: computed,
  estimatedPrice: computed,
  targetGroup: computed,
  rating: computed,
  commentsCount: computed,
  image: computed,
  destinations: computed,
  chartColors: computed,
  chartData: computed,
  potentialShocks: computed,
  packages: computed,
  costs: computed,
  allImages: computed,
  setDestination: action,
});
