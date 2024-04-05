const path = require('node:path')
const fs = require('node:fs').promises
const util = require('node:util')

class AttachmentStore {
	constructor (path = './') {
		this.path = path
	}

	async write (attachment, data) {
		const digest = this.toBase64(attachment.digest)
		const target = path.join(this.path, digest)
		try {
			await fs.stat(target)
		} catch (error) {
			if (error.code === 'ENOENT') {
				const result = await fs.writeFile(target, Buffer.from(data,  null))
				// console.error('store.write', target, digest, result)
			} else {
				// console.error(digest, 'already exists')
			}
		}
	}

	async read (attachment) {
		const digest = this.toBase64(attachment.digest)
		const target = path.join(this.path, digest)
		return this.toBase64((await fs.readFile(target)))
	}

	toBase64 (data) {
		return Buffer.from(data).toString('base64')
	}
}

const store = new AttachmentStore()

async function attachmentBackupHandler ({docs}) {
	for (let doc of docs) {
		if (!doc._attachments) { continue }
		for await (let attachmentName of Object.keys(doc._attachments)) {
			const attachment = doc._attachments[attachmentName]
			const parameters = {
				db: this.dbName,
				docId: doc._id,
				attachmentName
			}
			const attachmentStream = await this.service.getAttachment(parameters)
			const attachmentData = await streamToBuffer(attachmentStream.result)
			const result = await store.write(attachment, attachmentData)
		}
	}
}

async function attachmentRestoreHandler (batch) {
	const { docs } = batch
	for (let doc of docs) {
		if (!doc._attachments) { continue }
		for await (let attachmentName of Object.keys(doc._attachments)) {
			const attachment = doc._attachments[attachmentName]
			attachment.data = await store.read(attachment)
			delete attachment.stub
			delete attachment.length
			delete attachment.revpos
		}
		// console.log(util.inspect(doc, null, 4))
	}
	// console.log(util.inspect(batch.docs, null, 4))
	return batch
}

module.exports.attachmentBackupHandler = attachmentBackupHandler
module.exports.attachmentRestoreHandler = attachmentRestoreHandler

// h/t: https://stackoverflow.com/questions/10623798/how-do-i-read-the-contents-of-a-node-js-stream-into-a-string-variable
function streamToBuffer (stream) {
  const chunks = [];
  return new Promise((resolve, reject) => {
	stream.on('data', (chunk) => chunks.push(Buffer.from(chunk)));
	stream.on('error', (err) => reject(err));
	stream.on('end', () => resolve(Buffer.concat(chunks)));
  })
}