import { VictoryLabel, VictoryPie, VictoryTooltip } from 'victory';
import React from 'react';

import { rem2px } from './theme';
import Card from './Card';
import theme from './theme/index';

const renderTooltip = (value, tooltipMode, t) => {
  switch (tooltipMode) {
    default:
    case 'description':
      return value.x.toUpperCase();
    case 'value':
      return Math.round(value.y);
    case 'both':
      return `${value.x.toUpperCase()} - ${Math.round(value.y)}`;
    case 'none':
      break;
  }
};

const labelShowing = (labelsMode, width, height) => {
  switch (labelsMode) {
    default:
    case 'internal':
      return Math.min(width / 2.75, height / 2.75);
    case 'external':
      break;
  }
};

const LabelAndTip = (p) => (
  <g>
    <VictoryLabel
      {...p}
      capHeight={1}
      lineHeight={theme.lineHeights.solid}
      style={{
        display: p.labelsMode,
        fill:
          p.labelsMode === 'external'
            ? theme.colors.on.background
            : p.datum.onColor,
        cursor: 'pointer',
        fontFamily: theme.fontFamily,
        fontSize: rem2px(theme.fontSizes[p.fontSize]),
      }}
    />
    <VictoryTooltip
      {...p}
      labelComponent={
        <VictoryLabel
          lineHeight={theme.lineHeights.copy}
          style={{
            fontFamily: theme.fontFamily,
            fontSize: rem2px(theme.fontSizes[p.fontSize]),
          }}
        />
      }
      style={{
        text: {
          color: p.datum.color,
        },
      }}
      text={(value) => renderTooltip(value, p.tooltipMode, p.t)}
      flyoutStyle={{
        display: p.tooltipMode,
        fill: p.datum.text,
        stroke: 'transparent',
      }}
    />
  </g>
);

LabelAndTip.defaultEvents = VictoryTooltip.defaultEvents;

const DonutChart = ({
  data,
  colors,
  tooltipMode,
  width,
  height,
  labelsMode,
  fontSize,
  angle,
  radiusFactor,
  t,
  ...props
}) => (
  <Card bg="transparent" boxShadow={0}>
    <VictoryPie
      width={width * 1.35}
      height={width * 1.35}
      data={data}
      colorScale={colors}
      innerRadius={Math.min(width / radiusFactor, height / radiusFactor)}
      labelRadius={() => labelShowing(labelsMode, width, height)}
      labels={(d) => (labelsMode === 'internal' ? d.x.toUpperCase() : d.name)}
      labelComponent={
        <LabelAndTip
          t={t}
          angle={angle}
          labelsMode={labelsMode}
          tooltipMode={tooltipMode}
          fontSize={fontSize}
          center={{ x: width / 2, y: height / 2 }}
        />
      }
      {...props}
    />
  </Card>
);

DonutChart.defaultProps = {
  width: 300,
  height: 300,
  tooltipMode: 'both',
  labelsMode: 'internal',
  fontSize: 4,
  angle: 0,
  radiusFactor: 3.85,
};

export default DonutChart;
