import compose from 'lodash/fp/compose';
import { inject, observer } from 'mobx-react';
import React from 'react';
import { withTranslation } from 'react-i18next';

import {
  Flex,
  FlexPanel,
  IconButton,
  MaxPanel,
  SlidingPanel,
  Text,
  VideoPlayer,
} from '../design-system';
import { TravelStyleButton } from './';

export const TravelStylePanel = ({
  open,
  toggle,
  activeTravelStyle,
  otherTravelStyles,
  setActiveTravelStyle,
  width = 600,
  t,
}) => (
  <SlidingPanel
    id="travel-styles"
    open={open}
    maxWidth={width}
    closeType="none"
  >
    <MaxPanel width={width} bg="background" vscrollable>
      <Flex
        p={3}
        justifyContent="space-between"
        alignItems="center"
        bg="surface"
      >
        <Flex alignItems="center">
          <TravelStyleButton
            width={50}
            height={40}
            travelStyle={activeTravelStyle}
          />
          <Text ml={3} uppercase bold>
            {t(`styles_${activeTravelStyle.abbr}_name`, activeTravelStyle.name)}
          </Text>
        </Flex>
        <IconButton bg="surface" onClick={() => toggle()} name="close" />
      </Flex>

      <VideoPlayer width="100%" autoPlay={open} src={activeTravelStyle.video} />
      <FlexPanel p={4}>
        <Text lineHeight="copy">
          {t(`styles_${activeTravelStyle.abbr}_description`)}
        </Text>
        <Flex mt={3} alignItems="center" flexDirection="row">
          <Text bold uppercase mr={3} fontSize={4}>
            {t('other_styles', 'Other Styles')}
          </Text>
          {otherTravelStyles.map((ts) => (
            <TravelStyleButton
              width={50}
              height={50}
              m={2}
              key={ts.abbr}
              travelStyle={ts}
              onClick={() => setActiveTravelStyle(ts.abbr)}
            />
          ))}
        </Flex>
      </FlexPanel>
    </MaxPanel>
  </SlidingPanel>
);

export default compose(
  withTranslation('common'),
  inject(({ app }) => ({
    open: app.travelStylePanelToggle.isOpen,
    toggle: () => app.travelStylePanelToggle.toggle(),
    activeTravelStyle: app.activeTravelStyle,
    otherTravelStyles: app.otherTravelStyles,
    setActiveTravelStyle: (s) => app.setActiveTravelStyle(s),
  })),
  observer
)(TravelStylePanel);
