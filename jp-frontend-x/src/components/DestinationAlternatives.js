import React from 'react';

import { Alternatives, FlexPanel } from '../design-system';

export const DestinationAlternatives = ({ alternatives }) => (
  <FlexPanel
    bg="background"
    scrollable
    p={4}
    width={1}
    flex={1}
    flexWrap="wrap"
    flexFlow="row wrap"
    flexDirection="row"
    justifyContent="space-between"
  >
    {alternatives.map((al, key) => (
      <Alternatives key={key} name={al.name} imageUrl={al.image} />
    ))}
  </FlexPanel>
);

export default DestinationAlternatives;
