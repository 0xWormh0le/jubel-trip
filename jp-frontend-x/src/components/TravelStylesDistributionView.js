import React from 'react';
import { withTranslation } from 'react-i18next';

import { RatingDistributionView } from '../design-system';

export const TravelStylesDistributionView = ({ values, t }) => {
  const keys = Object.keys(values);
  const labels = keys.reduce(
    (o, k) => (o[k] = t(`styles_${k}_name`, k)) && o,
    {}
  );
  const colors = keys.reduce(
    (o, k) => (o[k] = `misc.branding.styles.${k}.bg`) && o,
    {}
  );

  return (
    <RatingDistributionView values={values} labels={labels} colors={colors} />
  );
};

export default withTranslation('common')(TravelStylesDistributionView);
