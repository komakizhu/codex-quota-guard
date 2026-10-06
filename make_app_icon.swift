import CoreGraphics
import Darwin
import Foundation
import ImageIO
import UniformTypeIdentifiers

guard CommandLine.arguments.count == 3,
      let pixelSize = Int(CommandLine.arguments[2]),
      pixelSize > 0 else {
    fputs("usage: make_app_icon.swift OUTPUT.png PIXEL_SIZE\n", stderr)
    exit(2)
}

let colorSpace = CGColorSpaceCreateDeviceRGB()
let context = CGContext(
    data: nil,
    width: pixelSize,
    height: pixelSize,
    bitsPerComponent: 8,
    bytesPerRow: pixelSize * 4,
    space: colorSpace,
    bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue
)!

let size = CGFloat(pixelSize)
context.setFillColor(CGColor(colorSpace: colorSpace, components: [1, 1, 1, 1])!)
context.fill(CGRect(x: 0, y: 0, width: size, height: size))

context.setStrokeColor(CGColor(colorSpace: colorSpace, components: [0, 0, 0, 1])!)
context.setLineWidth(size * 0.12)
context.setLineCap(.round)
context.addArc(
    center: CGPoint(x: size * 0.5, y: size * 0.5),
    radius: size * 0.29,
    startAngle: .pi / 3,
    endAngle: 5 * .pi / 3,
    clockwise: false
)
context.strokePath()

let outputURL = URL(fileURLWithPath: CommandLine.arguments[1])
guard let image = context.makeImage(),
      let destination = CGImageDestinationCreateWithURL(
        outputURL as CFURL,
        UTType.png.identifier as CFString,
        1,
        nil
      ) else {
    fputs("could not create PNG destination\n", stderr)
    exit(1)
}
CGImageDestinationAddImage(destination, image, nil)
guard CGImageDestinationFinalize(destination) else {
    fputs("could not write PNG\n", stderr)
    exit(1)
}
