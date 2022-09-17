import React from 'react';
import { inject, observer } from 'mobx-react';
import { withTranslation } from 'react-i18next';
import compose from 'lodash/fp/compose';
import ReactHintFactory from 'react-hint';
import ReactMarkdown from 'react-markdown';

import Box from '../design-system/Box';
import Icon from '../design-system/Icon';
import Link from '../design-system/Link';
import system from '../utils/System';
import Text from '../design-system/Text';

import 'react-hint/css/index.css';

const ReactHint = ReactHintFactory(React);

const MarkdownTooltipContainer = system(
  'MarkdownTooltipContainer',
  {
    extend: Box,
  },
  ({ theme }) => ({
    '.markdown-hint': {
      position: 'absolute',
      zIndex: '10000',
      cursor: 'default',
      animation: '0.5s fadeIn',
    },
    p: {
      padding: 0,
      lineHeight: theme.lineHeights.copy,
    },
    'p:first-child': {
      marginTop: 0,
    },
    'p:last-child': {
      marginBottom: 0,
    },
    '.country-hint': {
      position: 'absolute',
      zIndex: '10000',
      cursor: 'default',
      animation: '0.5s fadeIn',
    },
  })
);

const renderMarkdownTooltip = (renderers) => (target, content) => (
  <Box
    maxWidth={360}
    color="surface"
    bg="on.surface"
    boxShadow={4}
    borderRadius="sm"
    p={3}
  >
    <ReactMarkdown source={content} renderers={renderers} />
  </Box>
);

const renderCountryTooltip = (target, content) => (
  <Box
    maxWidth={560}
    bg="surface"
    color="on.surface"
    boxShadow={4}
    borderRadius="sm"
    p={3}
    m={2}
  >
    <Text fontSize={5} p={2} fontWeight="bold" mb={2} center>
      {target.dataset.rhCountryTitle}
    </Text>
    <Text fontSize={5} p={2}>
      <ReactMarkdown source={content} />
    </Text>
  </Box>
);

const renderStyleTooltip = (setActiveTravelStyle) => (target, content) =>
  renderMarkdownTooltip({
    link: ({ href, children, ...props }) => (
      <Link
        onClick={() => setActiveTravelStyle(href)}
        color={`misc.branding.styles.${href}.bg`}
        p={0}
        display="inline"
        {...props}
      >
        {children}
        <Icon
          name="arrow-play"
          size={18}
          inline
          style={{ verticalAlign: 'middle' }}
        />
      </Link>
    ),
  })(target, content);

const TooltipWrapper = ({ setActiveTravelStyle }) => {
  return (
    <>
      <MarkdownTooltipContainer>
        <ReactHint autoPosition events />
        <ReactHint
          persist
          attribute="data-rh-style"
          events={{ hover: true }}
          onRenderContent={renderStyleTooltip(setActiveTravelStyle)}
        />
        <ReactHint
          persist
          attribute="data-rh-markdown"
          className="markdown-hint"
          events={{
            hover: true,
          }}
          onRenderContent={renderMarkdownTooltip()}
        />
        <ReactHint
          persist
          attribute="data-rh-country"
          className="country-hint"
          position="right"
          events={{
            click: true,
          }}
          onRenderContent={renderCountryTooltip}
        />
      </MarkdownTooltipContainer>
    </>
  );
};

export default compose(
  withTranslation('common'),
  inject(({ app }) => ({
    setActiveTravelStyle: (s) => app.setActiveTravelStyle(s),
  })),
  observer
)(TooltipWrapper);
