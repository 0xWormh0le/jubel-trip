import FocusTrap from 'focus-trap-react';
import React from 'react';
import ReactDOM from 'react-dom';

import Absolute from './Absolute';
import Flex from './Flex';

const ModalPortal = ({
  children,
  close = () => false,
  zIndex = 9999,
  position,
  focusTrapOptions,
  ...props
}) =>
  ReactDOM.createPortal(
    <Flex
      top={0}
      left={0}
      right={0}
      bottom={0}
      zIndex={zIndex}
      bg="misc.overlay.bg"
      position="absolute"
      alignItems="center"
      justifyContent="center"
    >
      <FocusTrap
        focusTrapOptions={{
          onDeactivate: close,
          clickOutsideDeactivates: true,
          ...focusTrapOptions,
        }}
      >
        <Absolute
          bg="surface"
          border={1}
          boxShadow={3}
          borderRadius="sm"
          zIndex={zIndex}
          {...position}
          {...props}
        >
          {children}
        </Absolute>
      </FocusTrap>
    </Flex>,
    document.body
  );

const Modal = ({ open, ...props }) => open && <ModalPortal {...props} />;

export default Modal;
