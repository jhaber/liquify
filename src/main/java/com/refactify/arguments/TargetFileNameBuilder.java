package com.refactify.arguments;

public class TargetFileNameBuilder {
    public String buildFilename(final ConversionArguments arguments) {
        String source = arguments.getSource();
        String baseFileName = source.substring(0, source.lastIndexOf("."));
        return String.format("%s.%s", baseFileName, arguments.getConversionType().getExtension());
    }
}
