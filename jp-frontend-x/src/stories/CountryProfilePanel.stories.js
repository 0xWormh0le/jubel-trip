/* eslint-disable import/first */
import React from 'react';
import { storiesOf } from '@storybook/react';
import { withTranslation } from 'react-i18next';
import { Provider, observer } from 'mobx-react';

import { MaxPanel } from '../design-system';
import { CountryProfilePanel } from '../components/CountryProfilePanel';
import TabsState from '../models/TabsState';
import ToggleState from '../models/ToggleState';

const TranslatedCountryProfilePanel = withTranslation('common')(
  observer(CountryProfilePanel)
);

const countryProfilePanelToggle = new ToggleState({ open: true });

const countryTabsState = new TabsState([
  { id: 'JP', name: 'Japan' },
  { id: 'TH', name: 'Thailand' },
]);

const currentTrip = {
  destinations: [
    {
      id: 28,
      gid: 1609350,
      continentCode: 'AS',
      countryCode: 'JP',
      continentName: 'Asia',
      countryName: 'Japan',
      images: [
        {
          caption: 'Tokyo',
          src:
            'https://jubelproduction.s3.amazonaws.com/uploads/destination/main_image/28/shutterstock_175851101.jpg',
          thumbnail:
            'https://jubelproduction.s3.amazonaws.com/uploads/destination/main_image/28/card_shutterstock_175851101.jpg',
        },
        {
          caption: 'Tokyo',
          src:
            'https://jubelproduction.s3.amazonaws.com/uploads/photo/image/969/shutterstock_268422755.jpg',
          thumbnail:
            'https://jubelproduction.s3.amazonaws.com/uploads/photo/image/969/thumb_shutterstock_268422755.jpg',
        },
      ],
    },
    {
      id: 108,
      gid: 1153671,
      continentCode: 'AS',
      countryCode: 'TH',
      continentName: 'Asia',
      countryName: 'Thailand',
      images: [
        {
          caption: 'Chiang Mai',
          src:
            'https://jubelproduction.s3.amazonaws.com/uploads/destination/main_image/108/shutterstock_553585522.jpg',
          thumbnail:
            'https://jubelproduction.s3.amazonaws.com/uploads/destination/main_image/108/card_shutterstock_553585522.jpg',
        },
        {
          caption: 'Chiang Mai',
          src:
            'https://jubelproduction.s3.amazonaws.com/uploads/photo/image/581/shutterstock_327800120.jpg',
          thumbnail:
            'https://jubelproduction.s3.amazonaws.com/uploads/photo/image/581/thumb_shutterstock_327800120.jpg',
        },
      ],
    },
  ],
};

const appState = {
  countryInfo: {
    countryCode: 'JP',
    name: 'Japan',
    description:
      'Japan is distinct - no culture has charmed the world quite like Japanese culture. Japanese animation, cuisine, quirky style, and tradition, there is just something endearing about it all. Japan is also indescribable. It has a quality that you won’t experience anywhere else in the world due to the country’s long history of isolation, where outside influences have hardly touched the country’s culture. Stepping onto Japanese soil defies all perceptions a first-time traveler here may have. The country upholds its long-lived traditions and values respect, order, and obedience in everyday life. Yet, it is also known as a modern hub, being the producer of some of the most advanced technology in the world.\n' +
      '\n' +
      'The country is also a place of extremes, with oppressive heat in the summertime and frigid temperatures in the wintertime in areas like Hokkaido, the northernmost of the Japanese islands. You can hike through fields of wildflowers, snorkel on a beach in Okinawa, or soak in an onsen after a ski trip during the winter. You can also choose to dine at some of the world’s finest restaurants and eat meticulously crafted dishes, or simply snack on the colorful range of inexpensive pre-packaged onigiri (rice balls), sando (Japanese sandwiches), or bento boxes.\n' +
      '\n' +
      'Many visitors like to stay in ryokans (typical Japanese inns) and sleep on tatami mats, or cozy up in capsule hotels during their escapades in the country. The major cities are buzzing and highly urbanized, and smaller towns and villages present a pervading stillness and spare beauty. Japan is animated with everlasting tradition and will leave you spoilt for choice. A bit of a challenge to adapt to customs, language, and a strong keep-to-themselves atmosphere, the challenge is certainly worth it.\n' +
      '\n' +
      'If visiting remote villages or smaller cities in Japan you may want to download a translator app to help you get around and communicate with the locals. Japanese locals are extremely polite, do not tip at restaurants, and like to conform to traditional standards. It is recommended to become familiar with table manners in Japan, as mealtimes call for good etiquette.',
    boundingBox: [
      122.93816165500019,
      24.212103583,
      153.98560631600017,
      45.52041250200013,
    ],
  },
  getCountryImages(countryCode) {
    return currentTrip.destinations
      .filter((d) => d.countryCode === countryCode)
      .reduce((a, d) => a.concat(d.images), []);
  },
  map: {
    initialized: true,
  },
};

storiesOf('CountryProfilePanel', module)
  .addDecorator((story) => (
    <Provider app={appState}>
      <MaxPanel centered>{story()}</MaxPanel>
    </Provider>
  ))
  .add('default', () => (
    <TranslatedCountryProfilePanel
      countryTabsState={countryTabsState}
      isOpen={countryProfilePanelToggle.isOpen}
      onClose={countryProfilePanelToggle.close}
      setCountry={(country) => countryTabsState.setActiveTab(country)}
      width={600}
    />
  ));
