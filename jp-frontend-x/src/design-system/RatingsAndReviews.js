import React from 'react';

import Flex from './Flex';
import RatingControl from './RatingControl';
import ReviewButton from './ReviewButton';

const RatingsAndReviews = ({
  size = 18,
  label,
  initialRating,
  color,
  fullColor,
  ml,
  onClickReview,
  fractions,
  emptyIcon,
  fullIcon,
  readonly,
  py = 0,
}) => (
  <Flex alignItems="baseline" py={py}>
    <RatingControl
      size={size}
      initialRating={initialRating}
      color={color}
      fullColor={fullColor}
      fractions={fractions}
      emptyIcon={emptyIcon}
      fullIcon={fullIcon}
      readonly={readonly}
    />
    <ReviewButton size={size} label={label} ml={ml} onClick={onClickReview} />
  </Flex>
);

export default RatingsAndReviews;
