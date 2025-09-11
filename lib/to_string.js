const idEnc = require("hypercore-id-encoding");
const hypCrypto = require("hypercore-crypto");

function recordToStr(record) {
  const discKey = hypCrypto.discoveryKey(record.key);
  return `DB Record for discovery key ${idEnc.normalize(discKey)} with priority: ${record.priority}. Announcing? ${record.announce}`;
}

function streamToStr(stream) {
  const pubKey = idEnc.normalize(stream.remotePublicKey);
  return `${pubKey}`;
}

function coreToInfo(core) {
  const discKey = hypCrypto.discoveryKey(core.key);
  return `Discovery key ${idEnc.normalize(discKey)} (${core.contiguousLength} / ${core.length}, ${core.peers.length} peers)`;
}

module.exports = {
  recordToStr,
  streamToStr,
  coreToInfo,
};
