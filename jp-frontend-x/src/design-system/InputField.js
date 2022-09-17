import React from 'react';

import Box from './Box';
import Input from './Input';
import Text from './Text';

const InputField = (props) => {
  const { label, ...inputProps } = props;

  return (
    <Box>
      {label && (
        <Text fontSize={4} disabled={props.disabled}>
          {label}
        </Text>
      )}
      <Input
        {...inputProps}
        border="inherit"
        borderBottom={1}
        borderColor="selects.primaryColor"
        boxShadow="none"
        px={0}
        py={2}
      />
    </Box>
  );
};

export default InputField;
