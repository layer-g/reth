use reth_primitives::{
    constants::MIN_PROTOCOL_BASE_FEE_U256,
    proofs::{self, EMPTY_LIST_HASH},
    Address, Block, Bloom, Bytes, Header, SealedBlock, TransactionSigned, UintTryTo, Withdrawal,
    H256, H64, U256, U64,
};
use reth_rlp::{Decodable, Encodable};
use serde::{ser::SerializeMap, Deserialize, Serialize, Serializer};

/// And 8-byte identifier for an execution payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct PayloadId(H64);

// === impl PayloadId ===

impl PayloadId {
    /// Creates a new payload id from the given identifier.
    pub fn new(id: [u8; 8]) -> Self {
        Self(H64::from(id))
    }
}

impl std::fmt::Display for PayloadId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// This structure maps for the return value of `engine_getPayloadV2` of the beacon chain spec.
///
/// See also: <https://github.com/ethereum/execution-apis/blob/main/src/engine/shanghai.md#engine_getpayloadv2>
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionPayloadEnvelope {
    /// Execution payload, which could be either V1 or V2
    ///
    /// V1 (_NO_ withdrawals) MUST be returned if the payload timestamp is lower than the Shanghai
    /// timestamp
    ///
    /// V2 (_WITH_ withdrawals) MUST be returned if the payload timestamp is greater or equal to
    /// the Shanghai timestamp
    #[serde(rename = "executionPayload")]
    pub payload: ExecutionPayload,
    /// The expected value to be received by the feeRecipient in wei
    #[serde(rename = "blockValue")]
    pub block_value: U256,
}

impl ExecutionPayloadEnvelope {
    /// Returns the [ExecutionPayload] for the `engine_getPayloadV1` endpoint
    pub fn into_v1_payload(mut self) -> ExecutionPayload {
        // ensure withdrawals are removed
        self.payload.withdrawals.take();
        self.payload
    }
}

/// This structure maps on the ExecutionPayload structure of the beacon chain spec.
///
/// See also: <https://github.com/ethereum/execution-apis/blob/6709c2a795b707202e93c4f2867fa0bf2640a84f/src/engine/paris.md#executionpayloadv1>
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionPayload {
    pub parent_hash: H256,
    pub fee_recipient: Address,
    pub state_root: H256,
    pub receipts_root: H256,
    pub logs_bloom: Bloom,
    pub prev_randao: H256,
    pub block_number: U64,
    pub gas_limit: U64,
    pub gas_used: U64,
    pub timestamp: U64,
    pub extra_data: Bytes,
    pub base_fee_per_gas: U256,
    pub block_hash: H256,
    pub transactions: Vec<Bytes>,
    /// Array of [`Withdrawal`] enabled with V2
    /// See <https://github.com/ethereum/execution-apis/blob/6709c2a795b707202e93c4f2867fa0bf2640a84f/src/engine/shanghai.md#executionpayloadv2>
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub withdrawals: Option<Vec<Withdrawal>>,
}

impl From<SealedBlock> for ExecutionPayload {
    fn from(value: SealedBlock) -> Self {
        let transactions = value
            .body
            .iter()
            .map(|tx| {
                let mut encoded = Vec::new();
                tx.encode(&mut encoded);
                encoded.into()
            })
            .collect();
        ExecutionPayload {
            parent_hash: value.parent_hash,
            fee_recipient: value.beneficiary,
            state_root: value.state_root,
            receipts_root: value.receipts_root,
            logs_bloom: value.logs_bloom,
            prev_randao: value.mix_hash,
            block_number: value.number.into(),
            gas_limit: value.gas_limit.into(),
            gas_used: value.gas_used.into(),
            timestamp: value.timestamp.into(),
            extra_data: value.extra_data.clone(),
            base_fee_per_gas: U256::from(value.base_fee_per_gas.unwrap_or_default()),
            block_hash: value.hash(),
            transactions,
            withdrawals: value.withdrawals,
        }
    }
}

/// Try to construct a block from given payload. Perform addition validation of `extra_data` and
/// `base_fee_per_gas` fields.
///
/// NOTE: The log bloom is assumed to be validated during serialization.
/// NOTE: Empty ommers, nonce and difficulty values are validated upon computing block hash and
/// comparing the value with `payload.block_hash`.
///
/// See <https://github.com/ethereum/go-ethereum/blob/79a478bb6176425c2400e949890e668a3d9a3d05/core/beacon/types.go#L145>
impl TryFrom<ExecutionPayload> for SealedBlock {
    type Error = PayloadError;

    fn try_from(payload: ExecutionPayload) -> Result<Self, Self::Error> {
        if payload.extra_data.len() > 32 {
            return Err(PayloadError::ExtraData(payload.extra_data))
        }

        if payload.base_fee_per_gas < MIN_PROTOCOL_BASE_FEE_U256 {
            return Err(PayloadError::BaseFee(payload.base_fee_per_gas))
        }

        let transactions = payload
            .transactions
            .iter()
            .map(|tx| TransactionSigned::decode(&mut tx.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        let transactions_root = proofs::calculate_transaction_root(transactions.iter());

        let withdrawals_root =
            payload.withdrawals.as_ref().map(|w| proofs::calculate_withdrawals_root(w.iter()));

        let header = Header {
            parent_hash: payload.parent_hash,
            beneficiary: payload.fee_recipient,
            state_root: payload.state_root,
            transactions_root,
            receipts_root: payload.receipts_root,
            withdrawals_root,
            logs_bloom: payload.logs_bloom,
            number: payload.block_number.as_u64(),
            gas_limit: payload.gas_limit.as_u64(),
            gas_used: payload.gas_used.as_u64(),
            timestamp: payload.timestamp.as_u64(),
            mix_hash: payload.prev_randao,
            base_fee_per_gas: Some(
                payload
                    .base_fee_per_gas
                    .uint_try_to()
                    .map_err(|_| PayloadError::BaseFee(payload.base_fee_per_gas))?,
            ),
            extra_data: payload.extra_data,
            // Defaults
            ommers_hash: EMPTY_LIST_HASH,
            difficulty: Default::default(),
            nonce: Default::default(),
        }
        .seal_slow();

        if payload.block_hash != header.hash() {
            return Err(PayloadError::BlockHash {
                execution: header.hash(),
                consensus: payload.block_hash,
            })
        }

        Ok(SealedBlock {
            header,
            body: transactions,
            withdrawals: payload.withdrawals,
            ommers: Default::default(),
        })
    }
}

#[derive(thiserror::Error, Debug)]
pub enum PayloadError {
    /// Invalid payload extra data.
    #[error("Invalid payload extra data: {0}")]
    ExtraData(Bytes),
    /// Invalid payload base fee.
    #[error("Invalid payload base fee: {0}")]
    BaseFee(U256),
    /// Invalid payload block hash.
    #[error("Invalid payload block hash. Execution: {execution}. Consensus: {consensus}")]
    BlockHash {
        /// The block hash computed from the payload.
        execution: H256,
        /// The block hash provided with the payload.
        consensus: H256,
    },
    /// Encountered decoding error.
    #[error(transparent)]
    Decode(#[from] reth_rlp::DecodeError),
}

/// This structure contains a body of an execution payload.
///
/// See also: <https://github.com/ethereum/execution-apis/blob/6452a6b194d7db269bf1dbd087a267251d3cc7f8/src/engine/shanghai.md#executionpayloadbodyv1>
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionPayloadBody {
    pub transactions: Vec<Bytes>,
    pub withdrawals: Vec<Withdrawal>,
}

impl From<Block> for ExecutionPayloadBody {
    fn from(value: Block) -> Self {
        let transactions = value.body.into_iter().map(|tx| {
            let mut out = Vec::new();
            tx.encode(&mut out);
            out.into()
        });
        ExecutionPayloadBody {
            transactions: transactions.collect(),
            withdrawals: value.withdrawals.unwrap_or_default(),
        }
    }
}

/// The execution payload body response that allows for `null` values.
pub type ExecutionPayloadBodies = Vec<Option<ExecutionPayloadBody>>;

/// This structure contains the attributes required to initiate a payload build process in the
/// context of an `engine_forkchoiceUpdated` call.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PayloadAttributes {
    pub timestamp: U64,
    pub prev_randao: H256,
    pub suggested_fee_recipient: Address,
    /// Array of [`Withdrawal`] enabled with V2
    /// See <https://github.com/ethereum/execution-apis/blob/6452a6b194d7db269bf1dbd087a267251d3cc7f8/src/engine/shanghai.md#payloadattributesv2>
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub withdrawals: Option<Vec<Withdrawal>>,
}

/// This structure contains the result of processing a payload
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PayloadStatus {
    #[serde(flatten)]
    pub status: PayloadStatusEnum,
    /// Hash of the most recent valid block in the branch defined by payload and its ancestors
    pub latest_valid_hash: Option<H256>,
}

impl PayloadStatus {
    pub fn new(status: PayloadStatusEnum, latest_valid_hash: Option<H256>) -> Self {
        Self { status, latest_valid_hash }
    }

    pub fn from_status(status: PayloadStatusEnum) -> Self {
        Self { status, latest_valid_hash: None }
    }

    pub fn with_latest_valid_hash(mut self, latest_valid_hash: H256) -> Self {
        self.latest_valid_hash = Some(latest_valid_hash);
        self
    }
}

impl Serialize for PayloadStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("status", self.status.as_str())?;
        map.serialize_entry("latestValidHash", &self.latest_valid_hash)?;
        map.serialize_entry("validationError", &self.status.validation_error())?;
        map.end()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PayloadStatusEnum {
    /// VALID is returned by the engine API in the following calls:
    ///   - newPayloadV1:       if the payload was already known or was just validated and executed
    ///   - forkchoiceUpdateV1: if the chain accepted the reorg (might ignore if it's stale)
    Valid,

    /// INVALID is returned by the engine API in the following calls:
    ///   - newPayloadV1:       if the payload failed to execute on top of the local chain
    ///   - forkchoiceUpdateV1: if the new head is unknown, pre-merge, or reorg to it fails
    Invalid {
        #[serde(rename = "validationError")]
        validation_error: String,
    },

    /// SYNCING is returned by the engine API in the following calls:
    ///   - newPayloadV1:       if the payload was accepted on top of an active sync
    ///   - forkchoiceUpdateV1: if the new head was seen before, but not part of the chain
    Syncing,

    /// ACCEPTED is returned by the engine API in the following calls:
    ///   - newPayloadV1: if the payload was accepted, but not processed (side chain)
    Accepted,
    InvalidBlockHash {
        #[serde(rename = "validationError")]
        validation_error: String,
    },
}

impl PayloadStatusEnum {
    /// Returns the string representation of the payload status.
    pub fn as_str(&self) -> &'static str {
        match self {
            PayloadStatusEnum::Valid => "VALID",
            PayloadStatusEnum::Invalid { .. } => "INVALID",
            PayloadStatusEnum::Syncing => "SYNCING",
            PayloadStatusEnum::Accepted => "ACCEPTED",
            PayloadStatusEnum::InvalidBlockHash { .. } => "INVALID_BLOCK_HASH",
        }
    }

    /// Returns the validation error if the payload status is invalid.
    pub fn validation_error(&self) -> Option<&str> {
        match self {
            PayloadStatusEnum::InvalidBlockHash { validation_error } |
            PayloadStatusEnum::Invalid { validation_error } => Some(validation_error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use reth_interfaces::test_utils::generators::{
        random_block, random_block_range, random_header,
    };
    use reth_primitives::{
        bytes::{Bytes, BytesMut},
        TransactionSigned, H256,
    };
    use reth_rlp::{Decodable, DecodeError};

    fn transform_block<F: FnOnce(Block) -> Block>(src: SealedBlock, f: F) -> ExecutionPayload {
        let unsealed = src.unseal();
        let mut transformed: Block = f(unsealed);
        // Recalculate roots
        transformed.header.transactions_root =
            proofs::calculate_transaction_root(transformed.body.iter());
        transformed.header.ommers_hash = proofs::calculate_ommers_root(transformed.ommers.iter());
        SealedBlock {
            header: transformed.header.seal_slow(),
            body: transformed.body,
            ommers: transformed.ommers.into_iter().map(Header::seal_slow).collect(),
            withdrawals: transformed.withdrawals,
        }
        .into()
    }

    #[test]
    fn payload_body_roundtrip() {
        for block in random_block_range(0..100, H256::default(), 0..2) {
            let unsealed = block.clone().unseal();
            let payload_body: ExecutionPayloadBody = unsealed.into();

            assert_eq!(
                Ok(block.body),
                payload_body
                    .transactions
                    .iter()
                    .map(|x| TransactionSigned::decode(&mut &x[..]))
                    .collect::<Result<Vec<_>, _>>(),
            );

            assert_eq!(block.withdrawals.unwrap_or_default(), payload_body.withdrawals);
        }
    }

    #[test]
    fn payload_validation() {
        let block = random_block(100, Some(H256::random()), Some(3), Some(0));

        // Valid extra data
        let block_with_valid_extra_data = transform_block(block.clone(), |mut b| {
            b.header.extra_data = BytesMut::zeroed(32).freeze().into();
            b
        });
        assert_matches!(TryInto::<SealedBlock>::try_into(block_with_valid_extra_data), Ok(_));

        // Invalid extra data
        let block_with_invalid_extra_data: Bytes = BytesMut::zeroed(33).freeze();
        let invalid_extra_data_block = transform_block(block.clone(), |mut b| {
            b.header.extra_data = block_with_invalid_extra_data.clone().into();
            b
        });
        assert_matches!(
            TryInto::<SealedBlock>::try_into(invalid_extra_data_block),
            Err(PayloadError::ExtraData(data)) if data == block_with_invalid_extra_data
        );

        // Zero base fee
        let block_with_zero_base_fee = transform_block(block.clone(), |mut b| {
            b.header.base_fee_per_gas = Some(0);
            b
        });
        assert_matches!(
            TryInto::<SealedBlock>::try_into(block_with_zero_base_fee),
            Err(PayloadError::BaseFee(val)) if val == U256::ZERO
        );

        // Invalid encoded transactions
        let mut payload_with_invalid_txs: ExecutionPayload = block.clone().into();
        payload_with_invalid_txs.transactions.iter_mut().for_each(|tx| {
            *tx = Bytes::new().into();
        });
        assert_matches!(
            TryInto::<SealedBlock>::try_into(payload_with_invalid_txs),
            Err(PayloadError::Decode(DecodeError::InputTooShort))
        );

        // Non empty ommers
        let block_with_ommers = transform_block(block.clone(), |mut b| {
            b.ommers.push(random_header(100, None).unseal());
            b
        });
        assert_matches!(
            TryInto::<SealedBlock>::try_into(block_with_ommers.clone()),
            Err(PayloadError::BlockHash { consensus, .. })
                if consensus == block_with_ommers.block_hash
        );

        // None zero difficulty
        let block_with_difficulty = transform_block(block.clone(), |mut b| {
            b.header.difficulty = U256::from(1);
            b
        });
        assert_matches!(
            TryInto::<SealedBlock>::try_into(block_with_difficulty.clone()),
            Err(PayloadError::BlockHash { consensus, .. }) if consensus == block_with_difficulty.block_hash
        );

        // None zero nonce
        let block_with_nonce = transform_block(block.clone(), |mut b| {
            b.header.nonce = 1;
            b
        });
        assert_matches!(
            TryInto::<SealedBlock>::try_into(block_with_nonce.clone()),
            Err(PayloadError::BlockHash { consensus, .. }) if consensus == block_with_nonce.block_hash
        );

        // Valid block
        let valid_block = block;
        assert_matches!(TryInto::<SealedBlock>::try_into(valid_block), Ok(_));
    }

    #[test]
    fn serde_payload_status() {
        let s = r#"{"status":"SYNCING","latestValidHash":null,"validationError":null}"#;
        let status: PayloadStatus = serde_json::from_str(s).unwrap();
        assert_eq!(status.status, PayloadStatusEnum::Syncing);
        assert!(status.latest_valid_hash.is_none());
        assert!(status.status.validation_error().is_none());
        assert_eq!(serde_json::to_string(&status).unwrap(), s);

        let full = s;
        let s = r#"{"status":"SYNCING","latestValidHash":null}"#;
        let status: PayloadStatus = serde_json::from_str(s).unwrap();
        assert_eq!(status.status, PayloadStatusEnum::Syncing);
        assert!(status.latest_valid_hash.is_none());
        assert!(status.status.validation_error().is_none());
        assert_eq!(serde_json::to_string(&status).unwrap(), full);
    }
}
