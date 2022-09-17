import React, { Fragment } from 'react';

import Grid from './Grid';
import Text from './Text';
import RatingControl from './RatingControl';
import targetedProps from '../utils/targetedProps';

const RatingDistributionView = ({
  labels,
  values,
  colors,
  fullColor,
  ...props
}) => {
  const ratingProps = targetedProps(props, 'rating');
  const labelProps = targetedProps(props, 'label');
  const data = Object.keys(values).map((k) => [k, values[k]]);

  return (
    <Grid
      gridTemplateColumns="2fr 1fr"
      gridColumnGap={1}
      gridRowGap={0}
      {...props}
    >
      {data.map(([key, value]) => (
        <Fragment key={key}>
          <Text uppercase {...labelProps}>
            {labels[key]}
          </Text>
          <RatingControl
            emptyIcon="empty-circle"
            fullIcon="circle"
            size={8}
            fullColor={
              colors[key] || fullColor || RatingControl.defaultProps.fullColor
            }
            {...ratingProps}
            readonly
            initialRating={value}
            start={0}
            stop={100}
            step={20}
          />
        </Fragment>
      ))}
    </Grid>
  );
};

export default RatingDistributionView;
