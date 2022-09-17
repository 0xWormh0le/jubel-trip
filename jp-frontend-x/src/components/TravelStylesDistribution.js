import React, { Fragment } from 'react';
import { withTranslation } from 'react-i18next';
import PropTypes from 'prop-types';

import { Grid, Text, RatingControl, DonutChart } from '../design-system';
import { numberString } from '../utils/propTypes';

export const TravelStylesDistribution = ({
  t,
  distribution,
  view,
  fontSize,
  width,
  ...pieChartProps
}) =>
  view === 'ratings' ? (
    <Grid
      width={width}
      gridTemplateColumns="2fr 1fr"
      gridColumnGap={1}
      gridRowGap={0}
    >
      {distribution.getData().map((ts, key) => (
        <Fragment key={key}>
          <Text uppercase fontSize={fontSize}>
            {t(ts.label, ts.name)}
          </Text>
          <RatingControl
            fractions={2}
            emptyIcon="empty-circle"
            fullIcon="circle"
            size={8}
            fullColor="secondary"
            readonly
            initialRating={ts.y / 10}
          />
        </Fragment>
      ))}
    </Grid>
  ) : (
    <DonutChart
      {...pieChartProps}
      data={distribution.getData()}
      colors={distribution.getColors()}
      t={t}
    />
  );

TravelStylesDistribution.propTypes = {
  t: PropTypes.func.isRequired,
  travelDistribution: PropTypes.object,
  view: PropTypes.oneOf(['pie', 'ratings']),
  fontSize: numberString,
  width: numberString,
};

export default withTranslation('common')(TravelStylesDistribution);
