import compose from 'lodash/fp/compose';
import { inject, observer } from 'mobx-react';
import React from 'react';
import { withTranslation } from 'react-i18next';

import { FlexPanel, ImageSorter, TextContent } from '../design-system';

const ScorableAlternativeAbout = ({ images, description, t }) => (
  <FlexPanel bg="background" fontSize={4} flex={1}>
    <ImageSorter height={300} borderRadius="none" images={images} />
    <TextContent text={description} />
  </FlexPanel>
);

export default compose(
  withTranslation('common'),
  inject(({ app }) => ({
    images: app.selectedRouteElement.images,
    description: app.selectedRouteElement.description,
  })),
  observer
)(ScorableAlternativeAbout);
