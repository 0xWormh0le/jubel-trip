import React from 'react';
import { inject, observer } from 'mobx-react';
import { withTranslation } from 'react-i18next';
import compose from 'lodash/fp/compose';
import PropTypes from 'prop-types';

import { FlexPanel, ImageSorter, TextContent } from '../design-system';

export const CountryProfile = ({ country, images, mapIsInitialized }) =>
  country ? (
    <FlexPanel bg="background" fontSize={4} flex={1}>
      {mapIsInitialized && (
        <ImageSorter
          height={300}
          minHeight={300}
          borderRadius="none"
          images={images}
        />
      )}
      <TextContent py={0} text={country.description} />
    </FlexPanel>
  ) : null;

CountryProfile.propTypes = {
  country: PropTypes.object,
  t: PropTypes.func.isRequired,
};

export default compose(
  withTranslation('common'),
  inject(({ app }, { countryCode }) => ({
    country: app.countryInfo,
    images: app.getCountryImages(countryCode),
    mapIsInitialized: app.map.initialized,
  })),
  observer
)(CountryProfile);
