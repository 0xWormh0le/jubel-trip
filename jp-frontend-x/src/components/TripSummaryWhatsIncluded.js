import compose from 'lodash/fp/compose';
import { inject, observer } from 'mobx-react';
import React from 'react';
import { withTranslation } from 'react-i18next';
import Box from '../design-system/Box';
import Flex from '../design-system/Flex';
import FlexPanel from '../design-system/FlexPanel';
import Text from '../design-system/Text';
import Hr from '../design-system/Hr';

const packages = [
  'Get paired 1-on-1 with a Jubel travel expert to tailor this trip to your budget and preferences',
  'All flights, accommodations & transfers',
  'Select activities/ tours',
  'Your Jubel Atlas:<br />A curated shortlist of insider recommendations for you to check out - including restaurants, nightlife, attractions, and day trips. Common best practices and precautions are also shared so that you can safely enjoy your adventure.',
  'Emergency customer support seven days a week (on select plans)',
  'Changes at no extra cost (where possible)',
  'All taxes & fees',
  'Money-back guarantee',
];

export const TripSummaryWhatsIncluded = ({ packages, t }) => (
  <FlexPanel vscrollable lineHeight="copy">
    <Text
      my={4}
      textStyle="uppercase"
      fontWeight="700"
      color="on.background"
      textAlign="center"
    >
      {t('trip_summary_whats_included', "So What's Included?")}
    </Text>
    {packages.map((item, index) => (
      <Box px={3} key={index}>
        <Flex key={index} py={2}>
          <Text fontWeight="700" fontSize={4} width={30}>
            {index + 1}.
          </Text>
          <Text
            flex={1}
            fontSize={4}
            dangerouslySetInnerHTML={{ __html: item }}
          />
        </Flex>
        <Hr />
      </Box>
    ))}
  </FlexPanel>
);

export default compose(
  withTranslation('common'),
  inject(({ app }) => ({
    packages,
  })),
  observer
)(TripSummaryWhatsIncluded);
