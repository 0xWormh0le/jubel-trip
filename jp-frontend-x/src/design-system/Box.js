import hoverColors from '../utils/hoverColors';
import system from '../utils/System';

const Box = system(
  'Box',
  {},
  // core
  'color',
  'fontSize',
  'space',
  'width',
  // typography
  'fontFamily',
  'fontWeight',
  'letterSpacing',
  'lineHeight',
  'textAlign',
  // layout
  'display',
  'height',
  'maxHeight',
  'maxWidth',
  'minHeight',
  'minWidth',
  'size',
  // flexbox
  'alignContent',
  'alignItems',
  'alignSelf',
  'flex',
  'flexBasis',
  'flexDirection',
  'flexWrap',
  'justifyContent',
  'justifyItems',
  'justifySelf',
  'order',
  // grid layout
  'gridArea',
  'gridAutoFlow',
  'gridAutoRows',
  'gridColumn',
  'gridColumnGap',
  'gridGap',
  'gridRow',
  'gridRowGap',
  'gridTemplateAreas',
  'gridTemplateColumns',
  'gridTemplateRows',
  // borders
  'border',
  'borderBottom',
  'borderColor',
  'borderLeft',
  'borderRadius',
  'borderRight',
  'borderStyle',
  'borderTop',
  'borderWidth',
  // position
  'bottom',
  'left',
  'position',
  'right',
  'top',
  'zIndex',
  // misc
  'background',
  'backgroundImage',
  'backgroundPosition',
  'backgroundRepeat',
  'backgroundSize',
  'boxShadow',
  'opacity',
  'overflow',
  'textStyle',
  'colorStyle',
  'buttonStyle',
  (props) => ({
    pointerEvents: props.noPointerEvents && 'none',
    display:
      props.inline &&
      (props.display || 'block').replace(/^(inline-)?/, 'inline-'),
    overflowY:
      ((props.showOverflow || props.showVerticalOverflow) && 'auto') ||
      ((props.hideOverflow || props.hideVerticalOverflow) && 'hidden') ||
      ((props.scrollable || props.vscrollable) && 'auto'),
    overflowX:
      ((props.showOverflow || props.showHorizontalOverflow) && 'auto') ||
      ((props.hideOverflow || props.hideHorizontalOverflow) && 'hidden') ||
      ((props.scrollable || props.hscrollable) && 'auto'),
    transform: props.transform,
    color: hoverColors(props),
    backgroundColor: hoverColors({ ...props, _bg: true }),
    borderColor: hoverColors({ ...props, _prop: 'borderColor', debug: true }),
    cursor: props.cursor || (props.hoverable && 'pointer'),
    '&:hover': props.hoverable && {
      color: hoverColors({ ...props, _hover: true }),
      backgroundColor: hoverColors({ ...props, _bg: true, _hover: true }),
      borderColor: hoverColors({
        ...props,
        _prop: 'borderColor',
        _hover: true,
      }),
      textDecoration: props.decorateOnHover,
    },
  }),
  (props) =>
    props.hidden && {
      position: 'absolute',
      left: '-10000px',
      top: 'auto',
      width: '1px',
      height: '1px',
      overflow: 'hidden',
    }
);

export default Box;
