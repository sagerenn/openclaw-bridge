/**
 * QR helpers — normalize whatever a channel plugin returns from its QR login
 * start/wait flow into a PNG data URL that can be embedded directly in HTML
 * (<img src="data:image/png;base64,...">) or sent over the WS protocol.
 *
 * Why this exists: plugins return the "QR value" (the URL/text the user must
 * scan), not an image. For example, the weixin plugin's loginWithQrStart
 * returns `qrDataUrl: qrcodeUrl` where `qrcodeUrl` is a plain https login URL
 * (the ilink qrcode_img_content). Embedding that string directly into an
 * <img src="..."> renders nothing — the browser tries to load the login URL as
 * an image and fails. Here we turn any non-data-URL value into a real QR image.
 */

import QRCode from "qrcode";
import { rootLogger } from "../util/logger.js";

const log = rootLogger.child("qr");

/** Matches a data URL carrying a raster/vector image (e.g. data:image/png;base64,....). */
const DATA_IMAGE_RE = /^data:image\/[a-zA-Z0-9.+-]+[,;]/i;

/**
 * Returns true if the given value is already an image data URL that can be
 * embedded directly into an <img src> without further processing.
 */
export function isImageDataUrl(value: unknown): value is string {
  return typeof value === "string" && DATA_IMAGE_RE.test(value);
}

/**
 * Normalize a QR value returned by a plugin into a PNG data URL.
 *
 * - If `value` is already an image data URL, it is returned unchanged.
 * - Otherwise (a plain URL or arbitrary text), a QR code PNG is generated
 *   encoding that value and returned as a data URL.
 * - If `value` is empty or encoding fails, returns undefined.
 */
export async function toQrImageDataUrl(value: unknown): Promise<string | undefined> {
  if (typeof value !== "string" || value.length === 0) return undefined;

  // Already a renderable image data URL — embed as-is.
  if (DATA_IMAGE_RE.test(value)) return value;

  try {
    const dataUrl = await QRCode.toDataURL(value, {
      errorCorrectionLevel: "M",
      margin: 2,
      width: 480,
    });
    log.debug("Generated QR data URL from plugin value", { valueLen: value.length });
    return dataUrl;
  } catch (err) {
    log.warn("Failed to generate QR image from value", { error: String(err) });
    return undefined;
  }
}
