import React from 'react';

import FlexPanel from './FlexPanel';
import IconButton from './IconButton';

const ThumbnailLink = ({ image, label, ...props }) => (
  <FlexPanel
    centered
    flex={1}
    borderRadius="sm"
    boxShadow={2}
    backgroundImage={`url(${image})`}
    backgroundRepeat="none"
    backgroundSize="cover"
    {...props}
  >
    <IconButton
      color="white"
      bg="transparent"
      name="arrow-play"
      size={64}
      label={label}
      labelPosition="bottom"
    />
  </FlexPanel>
);

export default ThumbnailLink;
