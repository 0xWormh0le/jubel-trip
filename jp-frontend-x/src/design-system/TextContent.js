import React from 'react';

import FlexPanel from './FlexPanel';
import Text from './Text';

const TextContent = ({ text, ...props }) => (
  <FlexPanel flex={1} p={4} vscrollable {...props}>
    <Text lineHeight="copy" dangerouslySetInnerHTML={{ __html: text }} />
  </FlexPanel>
);

export default TextContent;
