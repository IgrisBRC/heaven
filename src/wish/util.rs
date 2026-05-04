use crate::wish::Sin;

pub fn find_crlf(buffer: &[u8]) -> Option<usize> {
    buffer.windows(2).position(|w| w == b"\r\n")
}

pub fn bytes_to_i32(bytes: &[u8]) -> Result<i32, Sin> {
    if bytes.is_empty() {
        return Err(Sin::ParseError);
    }

    let (is_neg, start) = if bytes[0] == b'-' {
        (true, 1)
    } else {
        (false, 0)
    };

    let mut result = 0i32;
    for &b in &bytes[start..] {
        if !b.is_ascii_digit() {
            return Err(Sin::ParseError);
        }

        result = result
            .checked_mul(10)
            .and_then(|r| r.checked_add((b - b'0') as i32))
            .ok_or(Sin::ParseError)?;
    }

    if is_neg {
        result.checked_neg().ok_or(Sin::ParseError)
    } else {
        Ok(result)
    }
}

pub fn bytes_to_u64(bytes: &[u8]) -> Result<u64, Sin> {
    if bytes.is_empty() {
        return Err(Sin::ParseError);
    }

    let mut result = 0u64;

    for &b in bytes {
        if !b.is_ascii_digit() {
            return Err(Sin::ParseError);
        }

        result = result
            .checked_mul(10)
            .and_then(|r| r.checked_add((b - b'0') as u64))
            .ok_or(Sin::ParseError)?;
    }

    Ok(result)
}

pub fn bytes_to_i64(bytes: &[u8]) -> Result<i64, Sin> {
    if bytes.is_empty() {
        return Err(Sin::ParseError);
    }

    let (is_neg, start) = if bytes[0] == b'-' {
        (true, 1)
    } else {
        (false, 0)
    };

    let mut result = 0i64;
    for &b in &bytes[start..] {
        if !b.is_ascii_digit() {
            return Err(Sin::ParseError);
        }

        result = result
            .checked_mul(10)
            .and_then(|r| r.checked_add((b - b'0') as i64))
            .ok_or(Sin::ParseError)?;
    }

    if is_neg {
        result.checked_neg().ok_or(Sin::ParseError)
    } else {
        Ok(result)
    }
}

pub fn bytes_to_usize(bytes: &[u8]) -> Result<usize, Sin> {
    if bytes.is_empty() {
        return Err(Sin::ParseError);
    }

    let mut result = 0usize;

    for &b in bytes {
        if !b.is_ascii_digit() {
            return Err(Sin::ParseError);
        }

        result = result
            .checked_mul(10)
            .and_then(|r| r.checked_add((b - b'0') as usize))
            .ok_or(Sin::ParseError)?;
    }

    Ok(result)
}
